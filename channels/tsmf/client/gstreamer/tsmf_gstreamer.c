/*
 * FreeRDP: A Remote Desktop Protocol Implementation
 * Video Redirection Virtual Channel - GStreamer Decoder
 *
 * (C) Copyright 2012 HP Development Company, LLC 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <X11/Xlib.h>
#include <X11/extensions/Xrandr.h>
#include <X11/extensions/shape.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>
#include <gst/interfaces/xoverlay.h>

#include <sys/types.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#include "tsmf_constants.h"
#include "tsmf_decoder.h"

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#define SHARED_MEM_KEY	7777
#define TRY_DECODEBIN	0
#define SEEK_TOLERANCE 10000000

typedef struct _TSMFGstreamerDecoder
{
	ITSMFDecoder iface;

	int media_type; /* TSMF_MAJOR_TYPE_AUDIO or TSMF_MAJOR_TYPE_VIDEO */

	TS_AM_MEDIA_TYPE tsmf_media_type; /* TSMF description of the media type, (without ExtraData) */

	GstCaps *gst_caps;  /* Gstreamer description of the media type */

	GstState state;

	GstElement *pipe;
	GstElement *src;
	GstElement *queue;
	GstElement *decbin;
	GstElement *outbin;
	GstElement *outconv;
	GstElement *outsink;
	GstElement *aVolume;

	BOOL paused;
	UINT64 last_sample_end_time;
	UINT64 last_sample_start_time;

	// Time delta between real stream playback time and gstreamer internal playback time
	UINT64 seek_offset;

	// Flag for whether or not this stream is synced to another stream of the opposite media type
	// If this stream is Audio then it is synced to a Video stream
	// Or of this stream is Video then it is synced to an Audio stream 
	BOOL synced;

	// Flag for whether or not the decoder is actively performing a sync
	BOOL seeking;

	Display *disp;
	int *xfwin;
	Window subwin;
	BOOL subwinMapped;
	int xOffset;
	int yOffset;
	BOOL offsetObtained;
	int linked;
	double gstVolume;
	BOOL gstMuted;

	int pipeline_start_time_valid; /* We've set the start time and have not reset the pipeline */
	int shutdown; /* The decoder stream is shutting down */
	pthread_mutex_t gst_mutex;

} TSMFGstreamerDecoder;

const char *NAME_GST_STATE_PLAYING = "GST_STATE_PLAYING";
const char *NAME_GST_STATE_PAUSED = "GST_STATE_PAUSED";
const char *NAME_GST_STATE_READY = "GST_STATE_READY";
const char *NAME_GST_STATE_NULL = "GST_STATE_NULL";
const char *NAME_GST_STATE_VOID_PENDING = "GST_STATE_VOID_PENDING";
const char *NAME_GST_STATE_OTHER = "GST_STATE_?";

static BOOL video_ready = FALSE;
static BOOL audio_ready = False;
static UINT64 aseek_time = 0;
static UINT64 vseek_time = 0;

static inline const GstClockTime tsmf_gstreamer_timestamp_ms_to_gst(UINT64 ms_timestamp)
{
	/* 
	 * Convert Microsoft 100ns timestamps to Gstreamer 1ns units.
	 */
	return (GstClockTime)(ms_timestamp * 100);
}

static const char *tsmf_gstreamer_state_name(GstState state)
{
	const char *name;

	if (state == GST_STATE_PLAYING) name = NAME_GST_STATE_PLAYING;
	else if (state == GST_STATE_PAUSED) name = NAME_GST_STATE_PAUSED;
	else if (state == GST_STATE_READY) name = NAME_GST_STATE_READY;
	else if (state == GST_STATE_NULL) name = NAME_GST_STATE_NULL;
	else if (state == GST_STATE_VOID_PENDING) name = NAME_GST_STATE_VOID_PENDING;
	else name = NAME_GST_STATE_OTHER;

	return name;
}

static int tsmf_gstreamer_pipeline_set_state(TSMFGstreamerDecoder * mdecoder, GstState desired_state)
{
	if (!mdecoder)
	{
		DEBUG_DVC("tsmf_gstreamer_pipeline_set_state called on NULL decoder. Returning.");
		return 0;
	}

	GstStateChangeReturn state_change;
	int keep_waiting;
	int timeout;
	GstState current_state;
	GstState pending_state;
	const char *name;
	const char *current_name;
	const char *pending_name;

	if (!mdecoder->pipe)
	{
		DEBUG_DVC("tsmf_gstreamer_pipeline_set_state called on a decoder with a NULL pipe. Returning.");
		return 0;  /* Just in case this is called during startup or shutdown when we don't expect it */
	}

	name = tsmf_gstreamer_state_name(desired_state); /* For debug */

	if (desired_state == mdecoder->state) 
	{
		DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) called but already in state (%s). Returning.", name, name);
		return 0;  /* Redundant request - Nothing to do */
	}

	if (mdecoder->media_type == TSMF_MAJOR_TYPE_VIDEO)
                DEBUG_DVC("tsmf_gstreamer_pipeline_set_state_VIDEO:");
        else
                DEBUG_DVC("tsmf_gstreamer_pipeline_set_state_AUDIO:");

	keep_waiting = 1;
 	state_change = gst_element_set_state (mdecoder->pipe, desired_state);

	/*
	 *	To avoid RDP session hang, set timeout for changing gstreamer state to maximum of 
	 *	2 seconds(100 20ms chunks). On each iteration While waiting for a state change to 
	 *	occur, the timeout is decremented by 1 and we can take 20ms complete the iteration:
	 *	usleep(10000) + gst_element_get_state timeout of (10 * GST_MSECOND) = 20ms.
	 */
	timeout = 100; // in 20ms increments

	while (keep_waiting) 
	{
		if (state_change == GST_STATE_CHANGE_FAILURE) 
		{	
			DEBUG_WARN("tsmf_gstreamer_pipeline_set_state(%s) GST_STATE_CHANGE_FAILURE.", name);
			keep_waiting = 0;
		} 
		else if (state_change == GST_STATE_CHANGE_SUCCESS)
		{
			DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) GST_STATE_CHANGE_SUCCESS.", name);
			mdecoder->state = desired_state;
			keep_waiting = 0;
		}
		else if (state_change == GST_STATE_CHANGE_NO_PREROLL)
		{
			DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) GST_STATE_CHANGE_NO_PREROLL.", name);
			keep_waiting = 0;
		}
		else if (state_change == GST_STATE_CHANGE_ASYNC)
		{
			DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) GST_STATE_CHANGE_ASYNC.", name);
	
			state_change = gst_element_get_state(mdecoder->pipe, &current_state, &pending_state, 10 * GST_MSECOND);
			current_name = tsmf_gstreamer_state_name(current_state);
			pending_name = tsmf_gstreamer_state_name(pending_state);

			if (current_state == desired_state)
			{
				DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) GST_STATE_CHANGE_SUCCESS.", name);
				mdecoder->state = desired_state;
				keep_waiting = 0;
			}
			else if (pending_state != desired_state)
			{
				DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) changing to %s instead.", name, pending_name);
				keep_waiting = 0;
			}
			else 
			{
				DEBUG_DVC("tsmf_gstreamer_pipeline_set_state(%s) Waiting - current %s pending %s.", name, current_name, pending_name);

				usleep(10000);
				timeout--;
				if (timeout <= 0)
				{
					DEBUG_WARN("tsmf_gstreamer_pipeline_set_state: TIMED OUT - failed to change state");
					keep_waiting = 0;
					break;
				}
			}
		}
		else
		{
			DEBUG_WARN("tsmf_gstreamer_pipeline_set_state: Unknown state change result - exiting.");
			keep_waiting = 0;
		}
	}
	return 0;
}

static BOOL tsmf_gstreamer_set_format(ITSMFDecoder * decoder, TS_AM_MEDIA_TYPE * media_type)
{
	TSMFGstreamerDecoder * mdecoder = (TSMFGstreamerDecoder *) decoder;
	if (!mdecoder)
			return FALSE;
	GstBuffer *gst_buf_cap_codec_data; /* Buffer to hold extra descriptive codec-specific caps data */

	DEBUG_DVC("tsmf_gstreamer_set_format: ");

	switch (media_type->MajorType)
	{
		case TSMF_MAJOR_TYPE_VIDEO:
			mdecoder->media_type = TSMF_MAJOR_TYPE_VIDEO;
			mdecoder->tsmf_media_type = *media_type; /* Structure copy */
			break;
		case TSMF_MAJOR_TYPE_AUDIO:
			mdecoder->media_type = TSMF_MAJOR_TYPE_AUDIO;
			mdecoder->tsmf_media_type = *media_type; /* Structure copy */
			break;
		default:
			return FALSE;
	}

	switch (media_type->SubType)
	{
		case TSMF_SUB_TYPE_WVC1:
			gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
			if (gst_buf_cap_codec_data != NULL) 
			{
				memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), 
					media_type->ExtraData, media_type->ExtraDataSize);
			}
			else
			{ 
				DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", 
					media_type->ExtraDataSize);
			} 
			mdecoder->gst_caps = gst_caps_new_simple ("video/x-wmv",
				"bitrate", G_TYPE_UINT, media_type->BitRate,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				"wmvversion", G_TYPE_INT, 3,
				"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
				"format", GST_TYPE_FOURCC, GST_MAKE_FOURCC ('W', 'V', 'C', '1'),
				//"framerate", GST_TYPE_FRACTION, media_type->SamplesPerSecond.Numerator, media_type->SamplesPerSecond.Denominator,
				NULL);
			break;
		case TSMF_SUB_TYPE_MP4S:
			if (media_type->ExtraDataSize > 0)
			{
				gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
				if (gst_buf_cap_codec_data != NULL) 
				{
					memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), media_type->ExtraData, media_type->ExtraDataSize);
				}
				else
				{ 
					DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", media_type->ExtraDataSize);
				} 
				mdecoder->gst_caps =  gst_caps_new_simple ("video/x-divx",
					"divxversion", G_TYPE_INT, 5,
					"bitrate", G_TYPE_UINT, media_type->BitRate,
					"width", G_TYPE_INT, media_type->Width,
					"height", G_TYPE_INT, media_type->Height,
					"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
					NULL);
			}
			else
			{
				mdecoder->gst_caps =  gst_caps_new_simple ("video/x-divx",
					"divxversion", G_TYPE_INT, 5,
					"bitrate", G_TYPE_UINT, media_type->BitRate,
					"width", G_TYPE_INT, media_type->Width,
					"height", G_TYPE_INT, media_type->Height,
					NULL);
			}
			break;
		case TSMF_SUB_TYPE_MP42:
			mdecoder->gst_caps =  gst_caps_new_simple ("video/x-msmpeg",
				"msmpegversion", G_TYPE_INT, 42,
				"bitrate", G_TYPE_UINT, media_type->BitRate,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				NULL);
			break;
		case TSMF_SUB_TYPE_MP43:
			mdecoder->gst_caps =  gst_caps_new_simple ("video/x-msmpeg",
				"bitrate", G_TYPE_UINT, media_type->BitRate,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				"format", GST_TYPE_FOURCC, GST_MAKE_FOURCC ('M', 'P', '4', '3'),
				NULL);
			break;
		case TSMF_SUB_TYPE_WMA9:
			if (media_type->ExtraDataSize > 0)
			{
				gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
				if (gst_buf_cap_codec_data != NULL) 
				{
					memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), media_type->ExtraData, media_type->ExtraDataSize);
				}
				else
				{ 
					DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", media_type->ExtraDataSize);
				} 
				mdecoder->gst_caps =  gst_caps_new_simple ("audio/x-wma",
					"wmaversion", G_TYPE_INT, 3,
					"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
					"channels", G_TYPE_INT, media_type->Channels,
					"bitrate", G_TYPE_INT, media_type->BitRate,
					"depth", G_TYPE_INT, media_type->BitsPerSample,
					"width", G_TYPE_INT, media_type->BitsPerSample,
					"block_align", G_TYPE_INT, media_type->BlockAlign,
					"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
					NULL);
			}
			else
			{
				mdecoder->gst_caps =  gst_caps_new_simple ("audio/x-wma",
					"wmaversion", G_TYPE_INT, 3,
					"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
					"channels", G_TYPE_INT, media_type->Channels,
					"bitrate", G_TYPE_INT, media_type->BitRate,
					"depth", G_TYPE_INT, media_type->BitsPerSample,
					"width", G_TYPE_INT, media_type->BitsPerSample,
					"block_align", G_TYPE_INT, media_type->BlockAlign,
					NULL);			
			}
			break;
		case TSMF_SUB_TYPE_WMA2:
			gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
			if (gst_buf_cap_codec_data != NULL) 
			{
				memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), 
					media_type->ExtraData, media_type->ExtraDataSize);
			}
			else
			{ 
				DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", 
					media_type->ExtraDataSize);
			} 
			
			mdecoder->gst_caps =  gst_caps_new_simple ("audio/x-wma",
				"wmaversion", G_TYPE_INT, 2,
				"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
				"channels", G_TYPE_INT, media_type->Channels,
				"bitrate", G_TYPE_INT, media_type->BitRate,
				"depth", G_TYPE_INT, media_type->BitsPerSample,
				"width", G_TYPE_INT, media_type->BitsPerSample,
				"block_align", G_TYPE_INT, media_type->BlockAlign,
				"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
				NULL);
			break;
		case TSMF_SUB_TYPE_MP3:
			mdecoder->gst_caps =  gst_caps_new_simple ("audio/mpeg",
				"mpegversion", G_TYPE_INT, 1,
				"mpegaudioversion", G_TYPE_INT, 1,
				"layer", G_TYPE_INT, 3,
				"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
				"channels", G_TYPE_INT, media_type->Channels,
				"parsed", G_TYPE_BOOLEAN, TRUE,
				NULL);
			break;
		case TSMF_SUB_TYPE_WMV1:
			mdecoder->gst_caps = gst_caps_new_simple ("video/x-wmv",
				"bitrate", G_TYPE_UINT, media_type->BitRate,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				"wmvversion", G_TYPE_INT, 1,
				"format", GST_TYPE_FOURCC, GST_MAKE_FOURCC ('W', 'M', 'V', '1'),
				NULL);
			break;
		case TSMF_SUB_TYPE_WMV2:
			gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
			if (gst_buf_cap_codec_data != NULL) 
			{
				memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), 
					media_type->ExtraData, media_type->ExtraDataSize);
			}
			else
			{ 
				DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", 
					media_type->ExtraDataSize);
			} 
			mdecoder->gst_caps = gst_caps_new_simple ("video/x-wmv",
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				"wmvversion", G_TYPE_INT, 2,
				"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
				"format", GST_TYPE_FOURCC, GST_MAKE_FOURCC ('W', 'M', 'V', '2'),
				NULL);
			break;
		case TSMF_SUB_TYPE_WMV3:
			gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
			if (gst_buf_cap_codec_data != NULL) 
			{
				memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), 
					media_type->ExtraData, media_type->ExtraDataSize);
			}
			else
			{ 
				DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", 
					media_type->ExtraDataSize);
			} 
			mdecoder->gst_caps = gst_caps_new_simple ("video/x-wmv",
				"bitrate", G_TYPE_UINT, media_type->BitRate,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				"wmvversion", G_TYPE_INT, 3,
				"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
				"format", GST_TYPE_FOURCC, GST_MAKE_FOURCC ('W', 'M', 'V', '3'),
				//"framerate", GST_TYPE_FRACTION, media_type->SamplesPerSecond.Numerator, media_type->SamplesPerSecond.Denominator,
				NULL);
			break;
		case TSMF_SUB_TYPE_AVC1:
		case TSMF_SUB_TYPE_H264:
			if (media_type->ExtraDataSize > 0)
			{
				gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
				if (gst_buf_cap_codec_data != NULL) 
				{
					memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), media_type->ExtraData, media_type->ExtraDataSize);
				}
				else
				{ 
					DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", media_type->ExtraDataSize);
				} 
				mdecoder->gst_caps = gst_caps_new_simple ("video/x-h264",
					"width", G_TYPE_INT, media_type->Width,
					"height", G_TYPE_INT, media_type->Height,
					"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
					//"framerate", GST_TYPE_FRACTION, media_type->SamplesPerSecond.Numerator, media_type->SamplesPerSecond.Denominator,
					NULL);
			}
			else
			{
				mdecoder->gst_caps = gst_caps_new_simple ("video/x-h264",
					"width", G_TYPE_INT, media_type->Width,
					"height", G_TYPE_INT, media_type->Height,
					//"framerate", GST_TYPE_FRACTION, media_type->SamplesPerSecond.Numerator, media_type->SamplesPerSecond.Denominator,
					NULL);
			}
			break;
		case TSMF_SUB_TYPE_AC3:
			mdecoder->gst_caps =  gst_caps_new_simple ("audio/x-ac3",
				"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
				"channels", G_TYPE_INT, media_type->Channels,
				NULL);
			break;
		case TSMF_SUB_TYPE_AAC:
			/* For AAC the pFormat is a HEAACWAVEINFO struct, and the codec data
			   is at the end of it. See
			   http://msdn.microsoft.com/en-us/library/dd757806.aspx */
			if (media_type->ExtraData)
			{
				media_type->ExtraData += 12;
				media_type->ExtraDataSize -= 12;
			}
			gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
			if (gst_buf_cap_codec_data != NULL) 
			{
				memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), 
					media_type->ExtraData, media_type->ExtraDataSize);
			}
			else
			{ 
				DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", 
					media_type->ExtraDataSize);
			} 
			mdecoder->gst_caps = gst_caps_new_simple ("audio/mpeg",
				"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
				"channels", G_TYPE_INT, media_type->Channels,
				"mpegversion", G_TYPE_INT, 4,
				"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
				NULL);
			break;
		case TSMF_SUB_TYPE_MP1A:
			if (media_type->ExtraDataSize > 0)
			{
				gst_buf_cap_codec_data = gst_buffer_try_new_and_alloc(media_type->ExtraDataSize);
				if (gst_buf_cap_codec_data != NULL) 
				{
					memcpy(GST_BUFFER_MALLOCDATA(gst_buf_cap_codec_data), media_type->ExtraData, media_type->ExtraDataSize);
				}
				else
				{ 
					DEBUG_WARN("tsmf_gstreamer_set_format: gst_buffer_try_new_and_alloc(%d) failed.", media_type->ExtraDataSize);
				} 
				mdecoder->gst_caps = gst_caps_new_simple ("audio/mpeg",
					"mpegversion", G_TYPE_INT, 1,
					"channels", G_TYPE_INT, media_type->Channels,
					"codec_data", GST_TYPE_BUFFER, gst_buf_cap_codec_data,
					NULL);
			}
			else
			{
				mdecoder->gst_caps =  gst_caps_new_simple ("audio/mpeg",
					"mpegversion", G_TYPE_INT, 1,
					"channels", G_TYPE_INT, media_type->Channels,
				NULL);
			}
			break;
		case TSMF_SUB_TYPE_MP1V:
			mdecoder->gst_caps = gst_caps_new_simple ("video/mpeg",
				"mpegversion", G_TYPE_INT, 1,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				"systemstream", G_TYPE_BOOLEAN, FALSE,
				NULL);
			break;
		case TSMF_SUB_TYPE_YUY2:
			mdecoder->gst_caps = gst_caps_new_simple ("video/x-raw-yuv",
				"format", GST_TYPE_FOURCC, GST_MAKE_FOURCC ('Y', 'U', 'Y', '2'),
				//"bitrate", G_TYPE_UINT, media_type->BitRate,
				"width", G_TYPE_INT, media_type->Width,
				"height", G_TYPE_INT, media_type->Height,
				NULL);
			break;
		case TSMF_SUB_TYPE_MP2V:
			mdecoder->gst_caps = gst_caps_new_simple ("video/mpeg",
				//"bitrate", G_TYPE_UINT, media_type->BitRate,
				//"width", G_TYPE_INT, media_type->Width,
				//"height", G_TYPE_INT, media_type->Height,
				"mpegversion", G_TYPE_INT, 2,
				"systemstream", G_TYPE_BOOLEAN, FALSE,
				NULL);
			break;
		case TSMF_SUB_TYPE_MP2A:
			mdecoder->gst_caps =  gst_caps_new_simple ("audio/mpeg",
				"mpegversion", G_TYPE_INT, 2,
				"rate", G_TYPE_INT, media_type->SamplesPerSecond.Numerator,
				"channels", G_TYPE_INT, media_type->Channels,
				NULL);
			break;
		default:
			DEBUG_WARN("tsmf_gstreamer_set_format: unknown format:(%d).", media_type->SubType);
			return FALSE;
	}

	return TRUE;
}

static void tsmf_gstreamer_pipeline_send_end_of_stream(TSMFGstreamerDecoder * mdecoder)
{
	DEBUG_DVC("tsmf_gstreamer_pipeline_send_end_of_stream: ");

	if (mdecoder && mdecoder->src)
	{
		gst_app_src_end_of_stream(GST_APP_SRC(mdecoder->src));
	}

	return;
}

#ifdef __arm__
/* code from TI to check whether OMX is being lock or not */
static BOOL tsmf_gstreamer_pipeline_omx_available()
{
	BOOL ret = TRUE;
	int shm_fd = 0;
	struct shm_info
	{
		pid_t pid;
	}shm_info;
	struct shm_info *info = NULL;

	shm_fd = shm_open ("gstomx", (O_CREAT | O_RDWR), (S_IREAD | S_IWRITE));
	if (shm_fd < 0)
	{
		DEBUG_DVC("ERROR: failed to open shm");
		goto exit;
	}

	/* set file size */
	ftruncate(shm_fd, sizeof(struct shm_info));

	if ((info = mmap(0, sizeof(struct shm_info), (PROT_READ | PROT_WRITE), MAP_SHARED, shm_fd, 0)) == MAP_FAILED)
	{
		DEBUG_DVC("ERROR: failed to map");
		goto exit;
	}

	if (info->pid)
	{
		DEBUG_DVC ("ERROR: omxcore is in use by '%d'", info->pid);
		ret = FALSE;
	}
	else
	{
		DEBUG_DVC ("omxcore is available for use");
	}


	exit:
	if (info)
		munmap (info, sizeof(struct shm_info));

	if (shm_fd)
		close (shm_fd);

	return ret;
}
#endif

static void tsmf_gstreamer_clean_up(TSMFGstreamerDecoder * mdecoder, BOOL do_unref)
{
	// Cleaning up elements
	if (!mdecoder)
		return;

	if (do_unref)
	{
		if (mdecoder->src)
			gst_object_unref(mdecoder->src);
		
		if (mdecoder->queue)
			gst_object_unref(mdecoder->queue);
		
		if (mdecoder->decbin)
			gst_object_unref(mdecoder->decbin);

		if(mdecoder->outbin)
			gst_object_unref(mdecoder->outbin);

		if (mdecoder->outconv)
			gst_object_unref(mdecoder->outconv);

		if (mdecoder->outsink)
			gst_object_unref(mdecoder->outsink);

		if (mdecoder->aVolume)
			gst_object_unref(mdecoder->aVolume);
	}

	mdecoder->src = NULL;
	mdecoder->queue = NULL;
	mdecoder->decbin = NULL;
	mdecoder->outbin = NULL;
	mdecoder->outconv = NULL;
	mdecoder->outsink = NULL;
	mdecoder->aVolume = NULL;
}


static BOOL tsmf_gstreamer_pipeline_build(TSMFGstreamerDecoder * mdecoder)
{
	if (!mdecoder)
		return FALSE;

	tsmf_gstreamer_clean_up(mdecoder,FALSE);

	if (mdecoder->pipe)
	{
		gst_object_unref(mdecoder->pipe);
		mdecoder->pipe = NULL;
	}

	GstPad *out_pad;
	mdecoder->pipe = gst_pipeline_new (NULL);
	if (!mdecoder->pipe)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to create new pipe");
		return FALSE;
	}

	BOOL OMXavailable = FALSE;

#ifdef __arm__
	OMXavailable = tsmf_gstreamer_pipeline_omx_available();
#endif

	/*
	 * On Atlas without this printf, we'll see Illegal instruction only with optimization level set to -O2.
	*/
	const char *blank = ""; 
	printf("%s", blank);

	BOOL hwaccelflu = FALSE;
	BOOL hwaccelomx = FALSE;

	switch (mdecoder->tsmf_media_type.SubType)
	{
		case TSMF_SUB_TYPE_WMA2:
			mdecoder->decbin = gst_element_factory_make ("fluwmadec", NULL);
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_wmav2", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: WMA2");
			break;
		case TSMF_SUB_TYPE_WMA9:
			mdecoder->decbin = gst_element_factory_make ("fluwmadec", NULL);
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_wmapro", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: WMA9 - WMA PRO version 3");
			break;
		case TSMF_SUB_TYPE_MP3:
			mdecoder->decbin = gst_element_factory_make ("flump3dec", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP3");
			break;
		case TSMF_SUB_TYPE_MP4S:
			if (OMXavailable)
			{
				mdecoder->decbin = gst_element_factory_make ("omx_mpeg4dec", NULL);
				if (mdecoder->decbin)
				{
					hwaccelomx = TRUE;
				}
			}
			else
				mdecoder->decbin = NULL;

			if(!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("flumpeg4vdec", NULL);
			if(!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_mpeg4", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP4S");
			break;
		case TSMF_SUB_TYPE_MP42:
			mdecoder->decbin = gst_element_factory_make ("ffdec_msmpeg4v2", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP42");
			break;
		case TSMF_SUB_TYPE_MP43:
			mdecoder->decbin = gst_element_factory_make ("ffdec_msmpeg4", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP43");
			break;
		case TSMF_SUB_TYPE_MP2V:
			if (OMXavailable)
			{
				mdecoder->decbin = gst_element_factory_make ("omx_mpeg2dec", NULL);
				if (mdecoder->decbin)
				{
					hwaccelomx = TRUE;
				}
			}
			else
				mdecoder->decbin = NULL;
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_mpeg2video", NULL);

			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MPEG2 Video");
			break;
		case TSMF_SUB_TYPE_WMV1:
			mdecoder->decbin = gst_element_factory_make ("ffdec_wmv1", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: WMV1");
			break;
		case TSMF_SUB_TYPE_WMV2:
			mdecoder->decbin = gst_element_factory_make ("ffdec_wmv2", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: WMV2");
			break;
		case TSMF_SUB_TYPE_WVC1:
		case TSMF_SUB_TYPE_WMV3:
			mdecoder->decbin = gst_element_factory_make ("fluvadec", NULL);
			if (mdecoder->decbin)
			{
				hwaccelflu = TRUE;
			}
			else
			{
				if (OMXavailable)
				{
					mdecoder->decbin = gst_element_factory_make ("omx_vc1dec", NULL);
					if (mdecoder->decbin)
						hwaccelomx = TRUE;
				}
				else
					mdecoder->decbin = NULL;
			}

			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("fluwmvdec", NULL);
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_wmv3", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: WMV3");
			break;
		case TSMF_SUB_TYPE_AVC1:
		case TSMF_SUB_TYPE_H264:
			mdecoder->decbin = gst_element_factory_make ("fluvadec", NULL);
			if (mdecoder->decbin)
			{
				hwaccelflu = TRUE;
			}
			else
			{
				if (OMXavailable)
				{
					mdecoder->decbin = gst_element_factory_make ("omx_h264dec", NULL);
					if (mdecoder->decbin)
						hwaccelomx = TRUE;
				}
				else
					mdecoder->decbin = NULL;
			}
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("fluh264dec", NULL);

			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_h264", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: H264");
			break;
		case TSMF_SUB_TYPE_AC3:
			mdecoder->decbin = gst_element_factory_make ("ffdec_ac3", NULL);
			//mdecoder->decbin = gst_element_factory_make ("ffdec_ac3", NULL);//no fluendo equivalent?
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: AC3");
			break;
		case TSMF_SUB_TYPE_AAC:
			mdecoder->decbin = gst_element_factory_make ("fluaacdec", NULL);
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("faad", NULL);
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("ffdec_aac", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: AAC");
			break;
		case TSMF_SUB_TYPE_MP2A:
			mdecoder->decbin = gst_element_factory_make ("fluaacdec", NULL);
			if (!mdecoder->decbin)
				mdecoder->decbin = gst_element_factory_make ("faad", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP2A");
			break;
		case TSMF_SUB_TYPE_MP1A:
			mdecoder->decbin = gst_element_factory_make ("flump3dec", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP1A");
			break;
		case TSMF_SUB_TYPE_MP1V:
			mdecoder->decbin = gst_element_factory_make ("ffdec_mpegvideo", NULL);
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: MP1V");
			break;
		default:
			DEBUG_WARN("tsmf_gstreamer_pipeline_build: Unsupported media type %d", mdecoder->tsmf_media_type.SubType);
			return FALSE;
	}
	if (!mdecoder->decbin)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to load decoder plugin");
		return FALSE;
	}

	switch (mdecoder->media_type)
	{
		case TSMF_MAJOR_TYPE_VIDEO:
		{
			mdecoder->outbin = gst_bin_new ("videobin");
			if (hwaccelflu)
			{
				mdecoder->outconv = gst_element_factory_make ("queue", "queuetosink"); 
				mdecoder->outsink = gst_element_factory_make ("fluvasink", "videosink");
			}
			else if(hwaccelomx)
			{
				mdecoder->outconv = gst_element_factory_make ("queue", "queuetosink"); 
				mdecoder->outsink = gst_element_factory_make ("gemxvimagesink", "videosink");
			}
			else
			{
				mdecoder->outconv = gst_element_factory_make ("ffmpegcolorspace", "vconv"); 
				mdecoder->outsink = gst_element_factory_make ("xvimagesink", "videosink");
			}
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: building Video Pipe");

			if (mdecoder->xfwin == (int *) -1)
				DEBUG_WARN("tsmf_gstreamer_entry: failed to assign pointer to the memory address - shmat()");
			else
			{
				if (!mdecoder->disp)
					mdecoder->disp = XOpenDisplay(NULL);

				if (!mdecoder->subwin)
				{
					XLockDisplay(mdecoder->disp);
					mdecoder->subwin = XCreateSimpleWindow(mdecoder->disp, *mdecoder->xfwin, 0, 0, 1, 1, 0, 0, 0);
					XMapWindow(mdecoder->disp, mdecoder->subwin);
					mdecoder->subwinMapped = TRUE;
					XSync(mdecoder->disp, FALSE);
					XUnlockDisplay(mdecoder->disp);
				}
			}
			mdecoder->aVolume = 0;
			break;
		}
		case TSMF_MAJOR_TYPE_AUDIO:
		{
			mdecoder->outbin = gst_bin_new ("audiobin"); 
			mdecoder->outconv = gst_element_factory_make ("audioconvert", "aconv"); 
			mdecoder->outsink = gst_element_factory_make ("alsasink", NULL); 
			mdecoder->aVolume = gst_element_factory_make ("volume", "AudioVol");
			if (mdecoder->aVolume)
			{
				g_object_set(mdecoder->aVolume, "mute", mdecoder->gstMuted, NULL);
				g_object_set(mdecoder->aVolume, "volume", mdecoder->gstVolume, NULL);
			}
			DEBUG_DVC("tsmf_gstreamer_pipeline_build: building Audio Pipe");
			break;
		}
		default:
		break;
	}
	if (!mdecoder->outconv)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to load media converter");
		tsmf_gstreamer_clean_up(mdecoder, TRUE);
		return FALSE;
	}
	if (!mdecoder->outsink)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to load xvimagesink plugin");
		tsmf_gstreamer_clean_up(mdecoder, TRUE);
		return FALSE;
	}

	mdecoder->src = gst_element_factory_make ("appsrc", NULL);
	mdecoder->queue = gst_element_factory_make ("queue2", NULL);
	g_object_set(G_OBJECT(mdecoder->queue), "use-buffering", FALSE, NULL);
	g_object_set(G_OBJECT(mdecoder->queue), "use-rate-estimate", FALSE, NULL);
	g_object_set(G_OBJECT(mdecoder->queue), "max-size-buffers", 0, NULL);
	g_object_set(G_OBJECT(mdecoder->queue), "max-size-bytes", 0, NULL);
	g_object_set(G_OBJECT(mdecoder->queue), "max-size-time", (guint64) 0, NULL); 
	g_object_set(G_OBJECT(mdecoder->outsink), "sync", TRUE, NULL); //synchronize on the clock
	g_object_set(G_OBJECT(mdecoder->outsink), "async", FALSE, NULL); //no async state changes
	g_object_set(G_OBJECT(mdecoder->src), "format", GST_FORMAT_TIME, NULL);
	g_object_set(G_OBJECT(mdecoder->src), "is-live", FALSE, NULL);
	g_object_set(G_OBJECT(mdecoder->src), "blocksize", 1024, NULL);
	g_object_set(G_OBJECT(mdecoder->outsink), "blocksize", 1024, NULL);
	gst_app_src_set_stream_type((GstAppSrc *) mdecoder->src, GST_APP_STREAM_TYPE_STREAM);
	gst_app_src_set_max_bytes((GstAppSrc *) mdecoder->src, (guint64) 0);//unlimited
	gst_app_src_set_size((GstAppSrc *) mdecoder->src, -1);
	gst_app_src_set_latency((GstAppSrc *) mdecoder->src, 0, -1);
	gst_app_src_set_caps((GstAppSrc *) mdecoder->src, mdecoder->gst_caps);

	if (mdecoder->media_type == TSMF_MAJOR_TYPE_VIDEO)
	{
		gst_base_sink_set_max_lateness((GstBaseSink *) mdecoder->outsink, 40000000); //nanoseconds
	}
	else
	{
		g_object_set(G_OBJECT(mdecoder->outsink), "drift-tolerance", (gint64) 40000, NULL); //microseconds
		g_object_set(G_OBJECT(mdecoder->outsink), "buffer-time", (gint64) 200000, NULL); //microseconds
		g_object_set(G_OBJECT(mdecoder->outsink), "latency-time", (gint64) 10000, NULL); //microseconds
		g_object_set(G_OBJECT(mdecoder->outsink), "slave-method", 1, NULL);
	}

	out_pad = gst_element_get_static_pad(mdecoder->outconv, "sink");

	gboolean linkResult = FALSE;
	gst_bin_add(GST_BIN(mdecoder->outbin), mdecoder->outconv);
	gst_bin_add(GST_BIN(mdecoder->outbin), mdecoder->outsink);
	if (mdecoder->aVolume)
	{
		gst_bin_add(GST_BIN(mdecoder->outbin), mdecoder->aVolume);
		linkResult = gst_element_link_many(mdecoder->outconv, mdecoder->aVolume, mdecoder->outsink, NULL);
	}
	else
	{
		linkResult = gst_element_link(mdecoder->outconv, mdecoder->outsink);
	}
	if (!linkResult)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to link these elements: converter->sink");
		tsmf_gstreamer_clean_up(mdecoder, TRUE);
		return FALSE;
	}

	gst_element_add_pad(mdecoder->outbin, gst_ghost_pad_new ("sink", out_pad));
	gst_object_unref(out_pad);

	gst_bin_add(GST_BIN(mdecoder->pipe), mdecoder->src);
	gst_bin_add(GST_BIN(mdecoder->pipe), mdecoder->queue);
	gst_bin_add(GST_BIN(mdecoder->pipe), mdecoder->decbin);
	gst_bin_add(GST_BIN(mdecoder->pipe), mdecoder->outbin);

	linkResult = gst_element_link_many(mdecoder->src, mdecoder->queue, mdecoder->decbin, NULL);
	if (!linkResult)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to link these elements: source->decoder");
		tsmf_gstreamer_clean_up(mdecoder, TRUE);
		return FALSE;
	}

	mdecoder->linked = gst_element_link(mdecoder->decbin, mdecoder->outbin);
	if (!mdecoder->linked)
	{
		DEBUG_WARN("tsmf_gstreamer_pipeline_build: Failed to link these elements: decoder->output_bin");
		tsmf_gstreamer_clean_up(mdecoder, TRUE);
		return FALSE;
	}
	
	if (GST_IS_X_OVERLAY (mdecoder->outsink))
	{
		if(mdecoder->subwin)
		{
			XLockDisplay(mdecoder->disp);
			// gst_overlay_set_xwindow_id was deprecated, so using gst_x_overlay_set_window_handle instead
			//gst_x_overlay_set_xwindow_id (GST_X_OVERLAY (mdecoder->outsink), mdecoder->subwin);
			gst_x_overlay_set_window_handle (GST_X_OVERLAY (mdecoder->outsink), mdecoder->subwin);
			XUnlockDisplay(mdecoder->disp);
		}
	}

	// hackish - mpeg1 and mpeg2 videos cant seem to buffer enough data quick enough in the start to prevent visual stepping
	// so we just delay the playback of synced stream by .5 seconds to help ensure consistent flow of data
	if (mdecoder->synced)
		usleep(500000);

	return TRUE;
}

static BOOL tsmf_gstreamer_decodeEx(ITSMFDecoder * decoder, const BYTE * data, UINT32 data_size, UINT32 extensions,
        			UINT64 start_time, UINT64 end_time, UINT64 duration)
{
	TSMFGstreamerDecoder * mdecoder = (TSMFGstreamerDecoder*) decoder;

	if (!mdecoder)
	{
		return FALSE;
	}

	int mutexret = pthread_mutex_lock(&mdecoder->gst_mutex);

	if (mutexret != 0)
		return FALSE;

	if (mdecoder->shutdown)
	{
		pthread_mutex_unlock(&mdecoder->gst_mutex);
		return FALSE;
	}

	GstBuffer *gst_buf;

	/* 
         * This function is always called from a stream-specific thread. 
         * It should be alright to block here if necessary.
         * We don't expect to block here often, since the pipeline should
	 * have more than enough buffering.
         */

	if (mdecoder->media_type == TSMF_MAJOR_TYPE_VIDEO)
	{
		DEBUG_DVC("tsmf_gstreamer_decodeEx_VIDEO. Start:(%"PRIu64") End:(%"PRIu64") Duration:(%"PRIu64") Last End:(%"PRIu64")",
				start_time, end_time, duration, mdecoder->last_sample_end_time);
	}
	else
	{
		DEBUG_DVC("tsmf_gstreamer_decodeEx_AUDIO. Start:(%"PRIu64") End:(%"PRIu64") Duration:(%"PRIu64") Last End:(%"PRIu64")",
				start_time, end_time, duration, mdecoder->last_sample_end_time);
	}

	if (mdecoder->gst_caps == NULL) 
	{
		DEBUG_WARN("tsmf_gstreamer_decodeEx: tsmf_gstreamer_set_format not called or invalid format.");
		pthread_mutex_unlock(&mdecoder->gst_mutex);
		return FALSE;
	}

	BOOL useTimestamps = TRUE;
	// Relative timestamping will sometimes be set to 0
	// so we ignore these timestamps just to be safe(bit 8)  
	if (extensions & 0x00000080)
	{
		DEBUG_DVC("Ignoring the timestamps - relative - bit 8");
		useTimestamps = FALSE;
	}

	// If no timestamps exist then we dont want to look at the timestamp values (bit 7)
	if (extensions & 0x00000040)
	{
		DEBUG_DVC("Ignoring the timestamps - none - bit 7");
		useTimestamps = FALSE;
	}

	if (mdecoder->pipe == NULL) 
	{
		if (!tsmf_gstreamer_pipeline_build(mdecoder))
		{
			if (mdecoder->pipe)
			{
				tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_NULL);
				tsmf_gstreamer_clean_up(mdecoder, FALSE);
				gst_object_unref(mdecoder->pipe);
				mdecoder->pipe = NULL;
			}
			pthread_mutex_unlock(&mdecoder->gst_mutex);
			return FALSE;
		}

		// Set stream to seeking for initial audio/video sync
		mdecoder->seeking = TRUE;

		// Set the seek offset if buffer has valid timestamps.
		if (useTimestamps)
			mdecoder->seek_offset = start_time;

		tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_READY);
		mdecoder->pipeline_start_time_valid = 0;
	}
	else
	{
		/*
		 *  this is to fix gstreamer's seeking forward/backward issue with live stream.
		 *  set the seeking tolerance to 1 second.
		*/

		// Base the condition for a seek to be based on start time only
		// WMV1 and WMV2 files in particular have bad end time and duration values
		UINT64 minTime = mdecoder->last_sample_start_time - SEEK_TOLERANCE;
		UINT64 maxTime = mdecoder->last_sample_start_time + SEEK_TOLERANCE;

		// Make sure the minTime stops at 0, should we be at the beginning of the stream 
		if (mdecoder->last_sample_start_time < SEEK_TOLERANCE)
			minTime = 0; 

		// If the start_time is valid and different from the previous start time by more than the seek tolerance, then we have a seek condition
		if (((start_time > maxTime) || (start_time < minTime)) && useTimestamps)
		{
			DEBUG_DVC("tsmf_gstreamer_decodeEx: start_time=[%llu] > last_sample_start_time=[%llu] OR ", start_time, mdecoder->last_sample_start_time);
			DEBUG_DVC("tsmf_gstreamer_decodeEx: start_time=[%llu] < last_sample_start_time=[%llu] with", start_time, mdecoder->last_sample_start_time);
			DEBUG_DVC("tsmf_gstreamer_decodeEX: a tolerance of [%llu] from the last sample", SEEK_TOLERANCE);
			DEBUG_DVC("tsmf_gstreamer_decodeEx: Stream seek detected - flushing element.");

			mdecoder->seeking = TRUE;

			// since there are issues trying to  make the gstreamer pipeline jump to the new start time after a seek
			// we just maintain a offset between realtime and gstreamer time
			mdecoder->seek_offset = start_time; 

			tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_READY);

			/*
			 * This is to fix the discrepancy between audio/video start time during a seek
			*/

			if (mdecoder->media_type == TSMF_MAJOR_TYPE_VIDEO)
			{
				vseek_time = start_time;
				DEBUG_DVC("vseek_time set to %llu", start_time);
			}
			else
			{
				aseek_time = start_time;
				DEBUG_DVC("aseek_time set to %llu", start_time);
			}
		}
	}

	if (!mdecoder->src)
	{
		pthread_mutex_unlock(&mdecoder->gst_mutex);
		DEBUG_WARN("tsmf_gstreamer_decodeEx: failed to construct pipeline correctly. Unable to push buffer to source element.");
		return FALSE;
	}

	// If performing a seek and NOT synced to another stream then just reset the sync flag and set pipeline to paused
	if ((mdecoder->seeking) && (!mdecoder->synced))
	{
		mdecoder->seeking = FALSE;
		tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_PAUSED);
	}

	// If performing a seek and synced to another stream then reset the sync flag and set pipeline to paused
	// BUT ALSO do extra logic since we are trying to keep the audio/video stream synced
	if ((mdecoder->seeking) && (mdecoder->synced))
	{
		mdecoder->seeking = FALSE;
		tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_PAUSED);

		// Set video as ready, wait for audio to be ready then deal with audio seek delta if necessary
		if (mdecoder->media_type == TSMF_MAJOR_TYPE_VIDEO)
		{
			int timeout = 0;

			// Set the video ready flag
			video_ready = TRUE;

			// wait for audio thread to set 
			while (audio_ready == FALSE)
			{
				timeout++;
				usleep(1000);
				// wait up to 1.5 seconds
				if (timeout > 1500)
					break;	
			}

			if (audio_ready == TRUE)
				audio_ready = FALSE;

			if (aseek_time > 0)
			{
				DEBUG_DVC("aseek_time = %llu is greater than 0", aseek_time);
				if (start_time > aseek_time)
				{
					UINT64 streamDelay = (start_time - aseek_time) / 10;

					// if the streamDelay is greater than 1.5 seconds then something is most likely wrong so cap it at 1.5 seconds
					if (streamDelay > 1500000)
						streamDelay = 1500000;

					// Only sleep if off by more than 1 ms
					if (streamDelay >= 1000)
					{
						DEBUG_DVC("VIDEO sleeping for delay of %llu due to aseek time of %llu and a start time of %llu", streamDelay, aseek_time, start_time);
						usleep(streamDelay);
						DEBUG_DVC("VIDEO sleep delay of %llu is DONE", streamDelay);
					}
				}
			}
			else
				DEBUG_DVC("aseek_time = %llu is less than or equal to 0", aseek_time);

			aseek_time = 0;

		}

		// Set audio as ready, wait for video to be ready then deal with video seek delta if necessary
		else if (mdecoder->media_type == TSMF_MAJOR_TYPE_AUDIO)
		{
			int timeout = 0;

			audio_ready = TRUE;
			
			while (video_ready == FALSE)
			{
				timeout++;
				usleep(1000);
				// wait up to 1.5 seconds
				if (timeout > 1500)
					break;
			}

			if (video_ready == TRUE)
				video_ready = FALSE;

			if (vseek_time > 0)
			{
				DEBUG_DVC("vseek_time = %llu is greater than 0", vseek_time);
				if (start_time > vseek_time)
				{
					UINT64 streamDelay = (start_time - vseek_time) / 10; 
					// if the streamDelay is greater than 1.5 seconds then something is most likely wrong so cap it at 1.5 seconds
					if (streamDelay > 1500000)
						streamDelay = 1500000;
       
					//Only sleep if off by more than 1 ms
					if (streamDelay >= 1000)
					{
						DEBUG_DVC("AUDIO sleeping for delay of %llu due to vseek time of %llu and a start time of %llu", streamDelay, vseek_time, start_time);
						usleep(streamDelay);
						DEBUG_DVC("AUDIO sleep delay of %llu is DONE", streamDelay);
					}
				}
			}
			else
				DEBUG_DVC("vseek_time = %llu is less than or equal to 0", vseek_time);

			vseek_time = 0;
		}
	}

	gst_buf = gst_buffer_try_new_and_alloc(data_size);
	if (gst_buf == NULL) 
	{
		pthread_mutex_unlock(&mdecoder->gst_mutex);
		DEBUG_WARN("tsmf_gstreamer_decodeEx: gst_buffer_try_new_and_alloc(%d) failed.", data_size);
		return FALSE; 
	}
	gst_buffer_set_caps(gst_buf, mdecoder->gst_caps);
	memcpy(GST_BUFFER_MALLOCDATA(gst_buf), data, data_size);

	if (useTimestamps)
		GST_BUFFER_TIMESTAMP(gst_buf) = tsmf_gstreamer_timestamp_ms_to_gst(start_time) - tsmf_gstreamer_timestamp_ms_to_gst(mdecoder->seek_offset);
	else
		GST_BUFFER_TIMESTAMP(gst_buf) = GST_CLOCK_TIME_NONE;
	GST_BUFFER_DURATION(gst_buf) = GST_CLOCK_TIME_NONE;//tsmf_gstreamer_timestamp_ms_to_gst(duration);
	GST_BUFFER_OFFSET(gst_buf) = GST_BUFFER_OFFSET_NONE;

	gst_app_src_push_buffer(GST_APP_SRC(mdecoder->src), gst_buf);

	// Should only update the last timestamps if the current ones are valid
	if (useTimestamps)
	{
		mdecoder->last_sample_start_time = start_time;
		mdecoder->last_sample_end_time = end_time;
	}

	if (!mdecoder->pipeline_start_time_valid) 
	{
		// Always set base/start time to 0. Will use seek offset to translate real buffer times
		// back to 0. This allows the video to be started from anywhere and the ability to handle seeks
		// without rebuilding the pipeline, etc. since that is costly
		gst_element_set_base_time(mdecoder->pipe, tsmf_gstreamer_timestamp_ms_to_gst(0));
		gst_element_set_start_time(mdecoder->pipe, tsmf_gstreamer_timestamp_ms_to_gst(0));
		mdecoder->pipeline_start_time_valid = 1;
	}

	if(GST_STATE(mdecoder->pipe) != GST_STATE_PLAYING)
	{
		DEBUG_DVC("State not currently playing");

		if (!mdecoder->paused)
		{
			// Remap the window here if needed
			if ((mdecoder->subwin) && (!mdecoder->subwinMapped))
			{
				XLockDisplay(mdecoder->disp);
				XMapWindow(mdecoder->disp, mdecoder->subwin);
				XSync(mdecoder->disp, FALSE);
				mdecoder->subwinMapped = TRUE;
				XUnlockDisplay(mdecoder->disp);
			}
			tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_PLAYING);
		}
	}
	pthread_mutex_unlock(&mdecoder->gst_mutex);
	return TRUE;
}

static void tsmf_gstreamer_change_volume(ITSMFDecoder * decoder, UINT32 newVolume, UINT32 muted)
{
	TSMFGstreamerDecoder * mdecoder = (TSMFGstreamerDecoder *) decoder;
	if (!mdecoder)
		return;

	if (mdecoder->shutdown)
		return;

	if (mdecoder->media_type == TSMF_MAJOR_TYPE_VIDEO)
		return;

	mdecoder->gstMuted = (BOOL) muted;
	DEBUG_DVC("tsmf_gstreamer_change_volume: mute=[%d]", mdecoder->gstMuted);
	mdecoder->gstVolume = (double) newVolume / (double) 10000;
	DEBUG_DVC("tsmf_gstreamer_change_volume: gst_new_vol=[%f]", mdecoder->gstVolume);

	if (!mdecoder->aVolume)
		return;

	if (!G_IS_OBJECT(mdecoder->aVolume))
		return;

	g_object_set(mdecoder->aVolume, "mute", mdecoder->gstMuted, NULL);
	g_object_set(mdecoder->aVolume, "volume", mdecoder->gstVolume, NULL);
}

static void tsmf_gstreamer_control(ITSMFDecoder * decoder, ITSMFControlMsg control_msg, UINT32 *arg)
{
	TSMFGstreamerDecoder * mdecoder = (TSMFGstreamerDecoder *) decoder;

	DEBUG_DVC("");

	if (!mdecoder)
		return;

	if (control_msg == Control_Pause) 
	{
		DEBUG_DVC("tsmf_gstreamer_control: Control_Pause");
		tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_PAUSED);
		mdecoder->paused = TRUE;
	}
	else if (control_msg == Control_Restart) 
	{
		DEBUG_DVC("tsmf_gstreamer_control: Control_Restart");
		mdecoder->paused = FALSE;

		// Only need to map the window if it is not currently mapped
		if ((mdecoder->subwin) && (!mdecoder->subwinMapped))
		{
			XLockDisplay(mdecoder->disp);
			XMapWindow(mdecoder->disp, mdecoder->subwin);
			mdecoder->subwinMapped = TRUE;
			XSync(mdecoder->disp, FALSE);
			XUnlockDisplay(mdecoder->disp);
		}

		if (mdecoder->pipeline_start_time_valid)
			tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_PLAYING);
	}
	else if (control_msg == Control_Flush) 
	{
		DEBUG_DVC("tsmf_gstreamer_control: Control_Flush");
		/* Reset stamps, flush buffers, etc */
		if (mdecoder->pipe) 
		{
			tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_NULL);
			tsmf_gstreamer_clean_up(mdecoder, FALSE);
			gst_object_unref(mdecoder->pipe);
			mdecoder->pipe = NULL;
		}
		mdecoder->seek_offset = 0;
		mdecoder->pipeline_start_time_valid = 0;
		mdecoder->paused = FALSE;
		
		// Only need to unmap window if it is currently mapped
		if ((mdecoder->subwin) && (mdecoder->subwinMapped))
		{
			XLockDisplay(mdecoder->disp);
			XUnmapWindow(mdecoder->disp, mdecoder->subwin);
			XSync(mdecoder->disp, FALSE);
			mdecoder->subwinMapped = FALSE;
			XUnlockDisplay(mdecoder->disp);
		}
	}
	else if (control_msg == Control_EndOfStream) 
	{
		mdecoder->paused = FALSE;
		DEBUG_DVC("tsmf_gstreamer_control: Control_EndOfStream");
		/* 
         *       The EOS may take some time to flow through the pipeline 
		 *       If the server sees the client "End of Stream Processed"
		 *       notification too soon, it may shut down the stream 
		 *       and clip the end of files.  
		 *       If that's the case, then we'll need to change the TSMF layer
		 *       to send the "End of Stream Processed" only after the stream
		 *       is truly EOS.
		 *       (It's unlikely we can simply "wait" here for it to happen
		 *       since we don't want to hold up acks, etc.)
		 */ 
		tsmf_gstreamer_pipeline_send_end_of_stream(mdecoder);
	}
}

static guint tsmf_gstreamer_buffer_level(ITSMFDecoder * decoder)
{
	TSMFGstreamerDecoder * mdecoder = (TSMFGstreamerDecoder *) decoder;
	DEBUG_DVC("tsmf_gstreamer_buffer_level\n");

	if (!mdecoder)
		return 0;

	if (mdecoder->shutdown)
		return 0;

	if (!G_IS_OBJECT(mdecoder->queue))
		return 0;

	guint clbuff = 0;
	g_object_get(mdecoder->queue, "current-level-buffers", &clbuff, NULL);
	DEBUG_DVC("Gstreamer Buffer Level: %i", clbuff);
	return clbuff;
}

static void tsmf_gstreamer_free(ITSMFDecoder * decoder)
{
	TSMFGstreamerDecoder * mdecoder = (TSMFGstreamerDecoder *) decoder;
	DEBUG_DVC("tsmf_gstreamer_free\n");

	if (mdecoder)
	{
		pthread_mutex_lock(&mdecoder->gst_mutex);
		mdecoder->shutdown = 1;
		if (mdecoder->pipe)
		{
			tsmf_gstreamer_pipeline_set_state(mdecoder, GST_STATE_NULL);
			tsmf_gstreamer_clean_up(mdecoder, FALSE);
			gst_object_unref(mdecoder->pipe);
			mdecoder->pipe = NULL;
		}
		if (mdecoder->gst_caps)
			gst_caps_unref(mdecoder->gst_caps);

		if (mdecoder->subwin)
		{
			DEBUG_DVC("destroy subwindow\n");
			XDestroyWindow(mdecoder->disp, mdecoder->subwin);
			XSync(mdecoder->disp, FALSE);
		}
		
		if (mdecoder->disp)
			XCloseDisplay(mdecoder->disp);

		aseek_time = 0;
		vseek_time = 0;
		video_ready = FALSE;
		audio_ready = FALSE;

		pthread_mutex_unlock(&mdecoder->gst_mutex);
		free(mdecoder);
		mdecoder = 0;
	}
}

static UINT64 tsmf_gstreamer_get_running_time(ITSMFDecoder * decoder)
{
	TSMFGstreamerDecoder *mdecoder = (TSMFGstreamerDecoder *) decoder;
	if (!mdecoder)
		return 0;
	if (!mdecoder->outsink)
		return mdecoder->last_sample_end_time;

	if(GST_STATE(mdecoder->pipe) != GST_STATE_PLAYING)
		return 0;

	GstFormat fmt = GST_FORMAT_TIME;
	gint64 pos = 0;
	gst_element_query_position (mdecoder->outsink, &fmt, &pos);
	DEBUG_DVC("tsmf_gstreamer_current_pos=[%"PRIu64"]", pos);
	return pos/100 + mdecoder->seek_offset;
}

static void tsmf_gstreamer_update_rendering_area(ITSMFDecoder * decoder, int newX, int newY, int newWidth, int newHeight, int numRectangles, RDP_RECT *rectangles)
{
	DEBUG_DVC("tsmf_gstreamer_update_rendering_area");
	TSMFGstreamerDecoder *mdecoder = (TSMFGstreamerDecoder *) decoder;
	if (!mdecoder)
		return;

	if (mdecoder->shutdown)
		return;

	if (GST_IS_X_OVERLAY (mdecoder->outsink))
	{
		if (!mdecoder->disp)
			mdecoder->disp = XOpenDisplay(NULL);

		XLockDisplay(mdecoder->disp);

		//multi-mon test
		int anewX = newX;
		int anewY = newY;
		if (!mdecoder->offsetObtained)
		{
			XSync(mdecoder->disp, FALSE);
			RROutput primary_output;
			XRRScreenResources *res = 0;
			int screen = 0;
			res = XRRGetScreenResourcesCurrent(mdecoder->disp, RootWindow(mdecoder->disp, screen));
			if (res)
			{
				DEBUG_DVC("number of output:%d", res->ncrtc);
				primary_output = XRRGetOutputPrimary(mdecoder->disp, DefaultRootWindow(mdecoder->disp));
				DEBUG_DVC("primary_output:%d", (int)primary_output);
				int i = 0;
				for (i = 0; i < res->ncrtc; i++)
				{
					XRRCrtcInfo *info = XRRGetCrtcInfo(mdecoder->disp, res, res->crtcs[i]);
					if (info)
					{
						if (info->noutput > 0)
						{
							if (info->outputs[0] == primary_output || i == 0)
							{
								mdecoder->xOffset = info->x;
								mdecoder->yOffset = info->y;
							}
							DEBUG_DVC("output %d ID: %lu (x,y): (%d,%d) (w,h): (%d,%d) primary: %d", i, info->outputs[0], info->x, info->y, info->width, info->height, (info->outputs[0] == primary_output));
						}
						XRRFreeCrtcInfo(info);
					}
				}
			}
			mdecoder->offsetObtained = TRUE;
		}
		anewX += mdecoder->xOffset;
		anewY += mdecoder->yOffset;
		
		XSync(mdecoder->disp, FALSE);
		//end of multi-mon test

		if(mdecoder->subwin)
		{
			XMoveWindow(mdecoder->disp, mdecoder->subwin, anewX, anewY);
			if(newWidth > 0 && newHeight > 0) {
				XResizeWindow(mdecoder->disp, mdecoder->subwin, newWidth, newHeight);
			} else {
				XResizeWindow(mdecoder->disp, mdecoder->subwin, 1, 1);
			}

			XSync(mdecoder->disp, FALSE);
			XShapeCombineRectangles (mdecoder->disp, mdecoder->subwin, ShapeBounding, 0, 0,(XRectangle*) rectangles, numRectangles, ShapeSet, Unsorted);
			XSync(mdecoder->disp, FALSE);
			//Sending Expose Event so freeRDP can do a redraw.
			XExposeEvent xpose;
			xpose.type = Expose;
			xpose.display = mdecoder->disp;
			xpose.window = *mdecoder->xfwin;
			xpose.x = 0;
			xpose.y = 0;
			XSendEvent(mdecoder->disp, *mdecoder->xfwin, TRUE, ExposureMask, (XEvent *)&xpose);
			XSync(mdecoder->disp, FALSE);
		}
		gst_x_overlay_expose (GST_X_OVERLAY (mdecoder->outsink));
		XUnlockDisplay(mdecoder->disp);
	}
}

static void tsmf_gstreamer_set_stream_sync(ITSMFDecoder * decoder, BOOL synced)
{
	TSMFGstreamerDecoder *mdecoder = (TSMFGstreamerDecoder *) decoder;

	mdecoder->synced = synced;
}



static int initialized = 0;

#ifdef STATIC_CHANNELS
#define freerdp_tsmf_client_decoder_subsystem_entry	gstreamer_freerdp_tsmf_client_decoder_subsystem_entry
#endif

ITSMFDecoder* freerdp_tsmf_client_decoder_subsystem_entry(void)
{
	TSMFGstreamerDecoder* decoder;

	if (!initialized)
	{
		gst_init(0, 0);
		initialized = 1;
	}

	decoder = malloc(sizeof(TSMFGstreamerDecoder));
	memset(decoder, 0, sizeof(TSMFGstreamerDecoder));

	decoder->iface.SetFormat = tsmf_gstreamer_set_format;
	decoder->iface.Decode = NULL;
	decoder->iface.GetDecodedData = NULL;
	decoder->iface.GetDecodedFormat = NULL;
	decoder->iface.GetDecodedDimension = NULL;
	decoder->iface.GetRunningTime = tsmf_gstreamer_get_running_time;
	decoder->iface.UpdateRenderingArea = tsmf_gstreamer_update_rendering_area;
	decoder->iface.Free = tsmf_gstreamer_free;
	decoder->iface.Control = tsmf_gstreamer_control;
	decoder->iface.DecodeEx = tsmf_gstreamer_decodeEx;
	decoder->iface.ChangeVolume = tsmf_gstreamer_change_volume;
	decoder->iface.BufferLevel = tsmf_gstreamer_buffer_level;
	decoder->iface.SetStreamSync = tsmf_gstreamer_set_stream_sync;
	decoder->paused = FALSE;
	decoder->subwinMapped = FALSE;
	decoder->subwin = 0;
	decoder->xOffset = 0;
	decoder->yOffset = 0;
	decoder->offsetObtained = FALSE;
	decoder->last_sample_start_time = 0;
	decoder->last_sample_end_time = 0;
	decoder->seek_offset = 0;
	decoder->seeking = FALSE;
	decoder->synced = FALSE;
	decoder->gstVolume = 0.5;
	decoder->gstMuted = FALSE;
	decoder->state = GST_STATE_VOID_PENDING;  /* No real state yet */
	pthread_mutex_init(&decoder->gst_mutex, NULL);

	int shmid = shmget(SHARED_MEM_KEY, sizeof(int), 0666);
	if (shmid < 0)
	{
		DEBUG_WARN("tsmf_gstreamer_entry: failed to get access to shared memory - shmget()");
	}
	else
	{
		decoder->xfwin = shmat(shmid, NULL, 0);
	}

	return (ITSMFDecoder *) decoder;
}
