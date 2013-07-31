/**
 * FreeRDP: A Remote Desktop Protocol Implementation
 * Video Redirection Virtual Channel - Media Container
 *
 * Copyright 2010-2011 Vic Lee
 * Copyright 2012 Hewlett-Packard Development Company, L.P.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifndef _WIN32
#include <sys/time.h>
#endif

#include <winpr/crt.h>
#include <winpr/synch.h>
#include <winpr/thread.h>
#include <winpr/collections.h>

#include <winpr/stream.h>
#include <freerdp/utils/list.h>
#include <freerdp/utils/event.h>
#include <freerdp/client/tsmf.h>

#include "tsmf_constants.h"
#include "tsmf_types.h"
#include "tsmf_decoder.h"
#include "tsmf_audio.h"
#include "tsmf_main.h"
#include "tsmf_codec.h"
#include "tsmf_media.h"

#include <pthread.h>

#define AUDIO_TOLERANCE 10000000LL
#define MAX_VIDEO_ACK_TIME 66667
#define MAX_ACKS_PER_CYCLE 2
#define ACK_TOLERANCE 1

struct _TSMF_PRESENTATION
{
	BYTE presentation_id[GUID_SIZE];

	const char* audio_name;
	const char* audio_device;

	UINT32 last_x;
	UINT32 last_y;
	UINT32 last_width;
	UINT32 last_height;
	UINT16 last_num_rects;
	RDP_RECT* last_rects;

	UINT32 output_x;
	UINT32 output_y;
	UINT32 output_width;
	UINT32 output_height;
	UINT16 output_num_rects;
	RDP_RECT* output_rects;

	IWTSVirtualChannelCallback* channel_callback;

	UINT64 audio_start_time;
	UINT64 audio_end_time;

	UINT32 volume;
	UINT32 muted;

	BOOL videoStreamPresent;
	BOOL audioStreamPresent;

	HANDLE mutex;
	HANDLE thread;

	LIST* stream_list;
};

struct _TSMF_STREAM
{
	UINT32 stream_id;

	TSMF_PRESENTATION* presentation;

	ITSMFDecoder* decoder;

	int major_type;
	int eos;
	UINT32 eos_message_id;
	IWTSVirtualChannelCallback* eos_channel_callback;
	int delayed_stop;

	UINT32 width;
	UINT32 height;

	ITSMFAudioDevice* audio;
	UINT32 sample_rate;
	UINT32 channels;
	UINT32 bits_per_sample;

	/* The end_time of last played sample */
	UINT64 last_end_time;

	/* The start time of last played sample */
	UINT64 last_start_time;

	/* Next sample should not start before this system time. */
	UINT64 next_start_time;

	BOOL started;
	BOOL thread_completed;

	HANDLE thread;
	HANDLE stopEvent;

	wQueue* sample_list;
	wQueue* sample_ack_list;
};

struct _TSMF_SAMPLE
{
	UINT32 sample_id;
	UINT64 start_time;
	UINT64 end_time;
	UINT64 duration;
	UINT32 extensions;
	UINT32 data_size;
	BYTE* data;
	UINT32 decoded_size;
	UINT32 pixfmt;
	BOOL invalidTimestamps;

	TSMF_STREAM* stream;
	IWTSVirtualChannelCallback* channel_callback;
	UINT64 ack_time;
};

static LIST* presentation_list = NULL;
static UINT64 last_played_audio_time = 0;
static HANDLE tsmf_mutex = NULL;
static int TERMINATING = 0;

static UINT64 get_current_time(void)
{
	struct timeval tp;

	gettimeofday(&tp, 0);
	return ((UINT64)tp.tv_sec) * 10000000LL + ((UINT64)tp.tv_usec) * 10LL;
}

static TSMF_SAMPLE* tsmf_stream_pop_sample(TSMF_STREAM* stream, int sync)
{
	TSMF_STREAM* s;
	LIST_ITEM* item;
	TSMF_SAMPLE* sample;
	BOOL pending = FALSE;
	TSMF_PRESENTATION* presentation = stream->presentation;

	if (Queue_Count(stream->sample_list) < 1)
		return NULL;

	if (sync)
	{
		if (stream->decoder)
		{
			if (stream->decoder->GetDecodedData)
			{
				if (stream->major_type == TSMF_MAJOR_TYPE_AUDIO)
				{
					/* Check if some other stream has earlier sample that needs to be played first */
					/* Use start time for comparisons as end times have proven to be less reliable across
					 * a wide range of test videos */
					if (stream->last_start_time > AUDIO_TOLERANCE)
					{
						WaitForSingleObject(presentation->mutex, INFINITE);

						for (item = presentation->stream_list->head; item; item = item->next)
						{
							s = (TSMF_STREAM*) item->data;

							if (s != stream && !s->eos && s->last_start_time &&
								s->last_start_time < stream->last_start_time - AUDIO_TOLERANCE)
							{
									DEBUG_DVC("Audio pending.");
									pending = TRUE;
									break;
							}
						}

						ReleaseMutex(presentation->mutex);
					}
				}
				else
				{
					if (stream->last_start_time > presentation->audio_start_time)
					{
						DEBUG_DVC("Video pending.");
						pending = TRUE;
					}
				}

			}
		}
	}

	if (pending)
		return NULL;

	sample = (TSMF_SAMPLE*) Queue_Dequeue(stream->sample_list);

	
	// Only update stream last end time if the sample end time is valid and greater than the current stream end time
	if (sample && (sample->end_time > stream->last_end_time) && (!sample->invalidTimestamps))
		stream->last_end_time = sample->end_time;

	// Only update stream last end time if the sample start time is valid and greater than the current stream start time     
	if (sample && (sample->start_time > stream->last_start_time) && (!sample->invalidTimestamps))
		stream->last_start_time = sample->start_time;

	return sample;
}

static void tsmf_sample_free(TSMF_SAMPLE* sample)
{
	if (sample->data)
		free(sample->data);

	free(sample);
}

static void tsmf_sample_ack(TSMF_SAMPLE* sample)
{
	tsmf_playback_ack(sample->channel_callback, sample->sample_id, sample->duration, sample->data_size);
}

static void tsmf_sample_queue_ack(TSMF_SAMPLE* sample)
{
	TSMF_STREAM* stream = sample->stream;

	Queue_Enqueue(stream->sample_ack_list, sample);
}

/* Returns TRUE if more acks are pending to be processed */
static BOOL tsmf_stream_process_ack(TSMF_STREAM* stream)
{
	TSMF_SAMPLE* sample;
	UINT64 ack_time;
	int i = 0;
	BOOL returnValue = FALSE;

	ack_time = get_current_time();

	while ((Queue_Count(stream->sample_ack_list) > 0) && !(WaitForSingleObject(stream->stopEvent, 0) == WAIT_OBJECT_0))
	{
		sample = (TSMF_SAMPLE*) Queue_Peek(stream->sample_ack_list);

		// End cycle if ack time is greater than current time plus tolerance
		if (!sample || (sample->ack_time > ack_time + ACK_TOLERANCE))
			break;

		// Only ack up to max acks per cycle
		if (i>=MAX_ACKS_PER_CYCLE)
		{
			returnValue = TRUE;
			break;
		}

		sample = Queue_Dequeue(stream->sample_ack_list);

		tsmf_sample_ack(sample);
		tsmf_sample_free(sample);
		i++;
	}

	return returnValue;
}

TSMF_PRESENTATION* tsmf_presentation_new(const BYTE* guid, IWTSVirtualChannelCallback* pChannelCallback)
{
	TSMF_PRESENTATION* presentation;
	pthread_t thid = pthread_self();
	char filename[4096] = "";
	snprintf(filename, 4096, "/tmp/tsmf-%i.tid", getpid());
	FILE* fout = NULL;
	fout = fopen(filename, "wt");
	
	if (fout)
	{
		fprintf(fout, "%lu\n", (unsigned long) (size_t) thid);
		fclose(fout);
	}

	presentation = tsmf_presentation_find_by_id(guid);

	if (presentation)
	{
		DEBUG_WARN("duplicated presentation id!");
		return NULL;
	}

	presentation = (TSMF_PRESENTATION*) malloc(sizeof(TSMF_PRESENTATION));
	ZeroMemory(presentation, sizeof(TSMF_PRESENTATION));

	memcpy(presentation->presentation_id, guid, GUID_SIZE);
	presentation->channel_callback = pChannelCallback;

	presentation->audioStreamPresent = FALSE;
	presentation->videoStreamPresent = FALSE;

	presentation->volume = 5000; /* 50% */
	presentation->muted = 0;

	presentation->mutex = CreateMutex(NULL, FALSE, NULL);
	presentation->stream_list = list_new();

	list_enqueue(presentation_list, presentation);

	return presentation;
}

TSMF_PRESENTATION* tsmf_presentation_find_by_id(const BYTE* guid)
{
	LIST_ITEM* item;
	TSMF_PRESENTATION* presentation;

	for (item = presentation_list->head; item; item = item->next)
	{
		presentation = (TSMF_PRESENTATION*) item->data;
		if (memcmp(presentation->presentation_id, guid, GUID_SIZE) == 0)
			return presentation;
	}
	return NULL;
}

static void tsmf_presentation_restore_last_video_frame(TSMF_PRESENTATION* presentation)
{
	RDP_REDRAW_EVENT* revent;

	if (presentation->last_width && presentation->last_height)
	{
		revent = (RDP_REDRAW_EVENT*) freerdp_event_new(TsmfChannel_Class, TsmfChannel_Redraw,
			NULL, NULL);

		revent->x = presentation->last_x;
		revent->y = presentation->last_y;
		revent->width = presentation->last_width;
		revent->height = presentation->last_height;

		if (!tsmf_push_event(presentation->channel_callback, (wMessage*) revent))
		{
			freerdp_event_free((wMessage*) revent);
		}

		presentation->last_x = 0;
		presentation->last_y = 0;
		presentation->last_width = 0;
		presentation->last_height = 0;
	}
}

static void tsmf_sample_playback_video(TSMF_SAMPLE* sample)
{
	UINT64 t;
	RDP_VIDEO_FRAME_EVENT* vevent;
	TSMF_STREAM* stream = sample->stream;
	TSMF_PRESENTATION* presentation = stream->presentation;

	DEBUG_DVC("MessageId %d EndTime %d data_size %d consumed.",
		sample->sample_id, (int)sample->end_time, sample->data_size);

	if (sample->data)
	{
		t = get_current_time();

		if (stream->next_start_time > t &&
			((sample->start_time >= presentation->audio_start_time) ||
			((sample->start_time < stream->last_start_time) && (!sample->invalidTimestamps))))
		{
			USleep((stream->next_start_time - t) / 10);
		}
		stream->next_start_time = t + sample->duration - 50000;

		if (presentation->last_x != presentation->output_x ||
			presentation->last_y != presentation->output_y ||
			presentation->last_width != presentation->output_width ||
			presentation->last_height != presentation->output_height ||
			presentation->last_num_rects != presentation->output_num_rects ||
			(presentation->last_rects && presentation->output_rects &&
			memcmp(presentation->last_rects, presentation->output_rects,
			presentation->last_num_rects * sizeof(RDP_RECT)) != 0))
		{
			tsmf_presentation_restore_last_video_frame(presentation);

			presentation->last_x = presentation->output_x;
			presentation->last_y = presentation->output_y;
			presentation->last_width = presentation->output_width;
			presentation->last_height = presentation->output_height;

			if (presentation->last_rects)
			{
				free(presentation->last_rects);
				presentation->last_rects = NULL;
			}

			presentation->last_num_rects = presentation->output_num_rects;

			if (presentation->last_num_rects > 0)
			{
				presentation->last_rects = malloc(presentation->last_num_rects * sizeof(RDP_RECT));
				ZeroMemory(presentation->last_rects, presentation->last_num_rects * sizeof(RDP_RECT));

				memcpy(presentation->last_rects, presentation->output_rects,
					presentation->last_num_rects * sizeof(RDP_RECT));
			}
		}

		vevent = (RDP_VIDEO_FRAME_EVENT*) freerdp_event_new(TsmfChannel_Class, TsmfChannel_VideoFrame,
			NULL, NULL);

		vevent->frame_data = sample->data;
		vevent->frame_size = sample->decoded_size;
		vevent->frame_pixfmt = sample->pixfmt;
		vevent->frame_width = sample->stream->width;
		vevent->frame_height = sample->stream->height;
		vevent->x = presentation->output_x;
		vevent->y = presentation->output_y;
		vevent->width = presentation->output_width;
		vevent->height = presentation->output_height;

		if (presentation->output_num_rects > 0)
		{
			vevent->num_visible_rects = presentation->output_num_rects;

			vevent->visible_rects = (RDP_RECT*) malloc(presentation->output_num_rects * sizeof(RDP_RECT));
			ZeroMemory(vevent->visible_rects, presentation->output_num_rects * sizeof(RDP_RECT));

			memcpy(vevent->visible_rects, presentation->output_rects,
				presentation->output_num_rects * sizeof(RDP_RECT));
		}

		/* The frame data ownership is passed to the event object, and is freed after the event is processed. */
		sample->data = NULL;
		sample->decoded_size = 0;

		if (!tsmf_push_event(sample->channel_callback, (wMessage*) vevent))
		{
			freerdp_event_free((wMessage*) vevent);
		}

#if 0
		/* Dump a .ppm image for every 30 frames. Assuming the frame is in YUV format, we
		   extract the Y values to create a grayscale image. */
		static int frame_id = 0;
		char buf[100];
		FILE * fp;
		if ((frame_id % 30) == 0)
		{
			snprintf(buf, sizeof(buf), "/tmp/FreeRDP_Frame_%d.ppm", frame_id);
			fp = fopen(buf, "wb");
			fwrite("P5\n", 1, 3, fp);
			snprintf(buf, sizeof(buf), "%d %d\n", sample->stream->width, sample->stream->height);
			fwrite(buf, 1, strlen(buf), fp);
			fwrite("255\n", 1, 4, fp);
			fwrite(sample->data, 1, sample->stream->width * sample->stream->height, fp);
			fflush(fp);
			fclose(fp);
		}
		frame_id++;
#endif
	}
}

static void tsmf_sample_playback_audio(TSMF_SAMPLE* sample)
{
	UINT64 latency = 0;
	TSMF_STREAM* stream = sample->stream;

	DEBUG_DVC("MessageId %d EndTime %d consumed.",
		sample->sample_id, (int)sample->end_time);

	if (sample->stream->audio && sample->data)
	{
		sample->stream->audio->Play(sample->stream->audio,
			sample->data, sample->decoded_size);
		sample->data = NULL;
		sample->decoded_size = 0;

		if (stream->audio && stream->audio->GetLatency)
			latency = stream->audio->GetLatency(stream->audio);
	}
	else
	{
		latency = 0;
	}

	sample->ack_time = latency + get_current_time();

	// Only update stream times if the sample timestamps are valid
	if (!sample->invalidTimestamps)
	{
		stream->last_end_time = sample->end_time + latency;
		stream->presentation->audio_start_time = sample->start_time + latency;
		stream->presentation->audio_end_time = sample->end_time + latency;
	}
}

static void tsmf_sample_playback(TSMF_SAMPLE* sample)
{
	BOOL ret = FALSE;
	UINT32 width;
	UINT32 height;
	UINT32 pixfmt = 0;
	TSMF_STREAM* stream = sample->stream;

	if (stream->decoder)
	{
		if (stream->decoder->DecodeEx)
			ret = stream->decoder->DecodeEx(stream->decoder, sample->data, sample->data_size, sample->extensions,
        			sample->start_time, sample->end_time, sample->duration);
		else
			ret = stream->decoder->Decode(stream->decoder, sample->data, sample->data_size, sample->extensions);
	}

	if (!ret)
	{
		tsmf_sample_ack(sample);
		tsmf_sample_free(sample);
		return;
	}

	free(sample->data);
	sample->data = NULL;

	if (stream->major_type == TSMF_MAJOR_TYPE_VIDEO)
	{
		if (stream->decoder->GetDecodedFormat)
		{
			pixfmt = stream->decoder->GetDecodedFormat(stream->decoder);
			if (pixfmt == ((UINT32) -1))
			{
				tsmf_sample_ack(sample);
				tsmf_sample_free(sample);
				return;
			}
			sample->pixfmt = pixfmt;
		}

		ret = FALSE ;
		if (stream->decoder->GetDecodedDimension)
		{
			ret = stream->decoder->GetDecodedDimension(stream->decoder, &width, &height);
			if (ret && (width != stream->width || height != stream->height))
			{
				DEBUG_DVC("video dimension changed to %d x %d", width, height);
				stream->width = width;
				stream->height = height;
			}
		}
	}

	if (stream->decoder->GetDecodedData)
	{
		sample->data = stream->decoder->GetDecodedData(stream->decoder, &sample->decoded_size);
		switch (sample->stream->major_type)
		{
			case TSMF_MAJOR_TYPE_VIDEO:
				tsmf_sample_playback_video(sample);
				tsmf_sample_ack(sample);
				tsmf_sample_free(sample);
				break;
			case TSMF_MAJOR_TYPE_AUDIO:
				tsmf_sample_playback_audio(sample);
				tsmf_sample_queue_ack(sample);
				break;
		}
	}
	else
	{
		TSMF_STREAM * stream = sample->stream;
		UINT64 ack_anticipation_time = get_current_time();
		UINT64 currentRunningTime = sample->start_time;
		UINT32 bufferLevel = 0;
		if (stream->decoder->GetRunningTime)
		{
			currentRunningTime = stream->decoder->GetRunningTime(stream->decoder);
		}
		if (stream->decoder->BufferLevel)
		{
			bufferLevel = stream->decoder->BufferLevel(stream->decoder);
		}
		switch (sample->stream->major_type)
		{
			case TSMF_MAJOR_TYPE_VIDEO:
			{
				TSMF_PRESENTATION * presentation = sample->stream->presentation;
				/*
				 *	Tell gstreamer that presentation screen area has moved.
				 *	So it can render on the new area.
				*/
				if (presentation->last_x != presentation->output_x || presentation->last_y != presentation->output_y ||
					presentation->last_width != presentation->output_width || presentation->last_height != presentation->output_height)
				{
					presentation->last_x = presentation->output_x;
					presentation->last_y = presentation->output_y;
					presentation->last_width = presentation->output_width;
					presentation->last_height = presentation->output_height;
					if(stream->decoder->UpdateRenderingArea)
					{
						stream->decoder->UpdateRenderingArea(stream->decoder, presentation->output_x, presentation->output_y,
						presentation->output_width, presentation->output_height, presentation->output_num_rects, presentation->output_rects);
					}
				}
				if ( presentation->last_num_rects != presentation->output_num_rects || (presentation->last_rects && presentation->output_rects &&
					memcmp(presentation->last_rects, presentation->output_rects, presentation->last_num_rects * sizeof(RDP_RECT)) != 0))
				{
					if (presentation->last_rects)
					{
						free(presentation->last_rects);
						presentation->last_rects = NULL;
					}

					presentation->last_num_rects = presentation->output_num_rects;

					if (presentation->last_num_rects > 0)
					{
						presentation->last_rects = malloc(presentation->last_num_rects * sizeof(RDP_RECT));
						ZeroMemory(presentation->last_rects, presentation->last_num_rects * sizeof(RDP_RECT));
						memcpy(presentation->last_rects, presentation->output_rects, presentation->last_num_rects * sizeof(RDP_RECT));
					}
					if(stream->decoder->UpdateRenderingArea)
					{
						stream->decoder->UpdateRenderingArea(stream->decoder, presentation->output_x, presentation->output_y,
						presentation->output_width, presentation->output_height, presentation->output_num_rects, presentation->output_rects);
					}
				}

				// Want to ensure we keep plenty of data in our buffers, so we ack quickly to build up our buffers
				if (bufferLevel < 10)
				{
					ack_anticipation_time += 0;
				}
				else if (bufferLevel < 20)
				{
					ack_anticipation_time += sample->duration;
				}
				else
				{
					if (currentRunningTime > sample->start_time)
					{
						ack_anticipation_time += (sample->duration < MAX_VIDEO_ACK_TIME) ? sample->duration : MAX_VIDEO_ACK_TIME;
					}
					else if(currentRunningTime == 0)
					{
						ack_anticipation_time += (sample->duration < MAX_VIDEO_ACK_TIME) ? sample->duration : MAX_VIDEO_ACK_TIME;
					}
					else
					{
						if (sample->invalidTimestamps)
							ack_anticipation_time += 0;
						else
							ack_anticipation_time += (sample->start_time - currentRunningTime);
					}
				}
				break;
			}
			case TSMF_MAJOR_TYPE_AUDIO:
			{
				last_played_audio_time = currentRunningTime;
				// Want to ensure we keep plenty of data in our buffers, so we ack quickly to build up our buffers
				if (bufferLevel < 10)
				{
					ack_anticipation_time += 0;
				}
				else if (bufferLevel < 20)
				{
					ack_anticipation_time += sample->duration;
				}
				else
				{
					if (currentRunningTime > sample->start_time)
					{
						ack_anticipation_time += sample->duration;
					}
					else if(currentRunningTime == 0)
					{
						ack_anticipation_time += sample->duration;
					}
					else
					{
						if (sample->invalidTimestamps)
							ack_anticipation_time += 0;
						else
							ack_anticipation_time += (sample->start_time - currentRunningTime);
					}
				}
				break;
			}
		}
		sample->ack_time = ack_anticipation_time;
		tsmf_sample_queue_ack(sample);
        }
}

static void* tsmf_stream_playback_func(void* arg)
{
	TSMF_SAMPLE* sample = NULL;
	TSMF_STREAM* stream = (TSMF_STREAM*) arg;
	TSMF_PRESENTATION* presentation = stream->presentation;
	BOOL acksPending = FALSE;
	UINT32 bufferLevel = 1;

	DEBUG_DVC("in %d", stream->stream_id);

	if (stream->major_type == TSMF_MAJOR_TYPE_AUDIO &&
		stream->sample_rate && stream->channels && stream->bits_per_sample)
	{
		if (stream->decoder)
		{
			if (stream->decoder->GetDecodedData)
			{
				stream->audio = tsmf_load_audio_device(
					presentation->audio_name && presentation->audio_name[0] ? presentation->audio_name : NULL,
					presentation->audio_device && presentation->audio_device[0] ? presentation->audio_device : NULL);
				if (stream->audio)
				{
					stream->audio->SetFormat(stream->audio,
						stream->sample_rate, stream->channels, stream->bits_per_sample);
				}
			}
		}
	}

	while (!(WaitForSingleObject(stream->stopEvent, 0) == WAIT_OBJECT_0))
	{
		BOOL processEOS = FALSE;
		acksPending = tsmf_stream_process_ack(stream);
		sample = tsmf_stream_pop_sample(stream, 1);

		if (sample)
			tsmf_sample_playback(sample);
		else
			USleep(5000);

		// If no acks are pending and we did not find a sample to process this cycle then sleep a little
		if (!acksPending && !sample)
			USleep(1000);

		// If an eos message was received then we will send a reply if all pending buffers have been loaded into the decoder
		// and the decoder has began processing all of them
		if (stream->eos)
		{
			if (stream->decoder->BufferLevel)
			{
				bufferLevel = stream->decoder->BufferLevel(stream->decoder);
			}
			else
				bufferLevel = 0;

			// If we have processed all pending buffers then we should process the eos
			if (!acksPending && !sample && (bufferLevel == 0))
			{
				// This gives an extra .2 seconds for buffers to flow through the decoder once its buffer level
				// has dropped down to 0
				USleep(200000);

				processEOS = TRUE;
			}
		}

		// If we have cleared local eos processing then we can send the response back to the server      
		if (processEOS)
		{
			// The thread should only be stopped during eos if we are restarting - which delays the eos response
			// since we will start the thread again
			if (!stream->thread_completed)
			{
				tsmf_send_eos_response(stream->eos_channel_callback, stream->eos_message_id);
				stream->eos = 0;
			}

			// If a stop was received after the EOS message then we should break as once the stop has been received then the stream is officially done
			// but if we have just processed an EOS then there is still a chance the stream can be seeked back to an earlier time on the server, so we 
			// dont break in that case
			if (stream->delayed_stop)
				break;
         
			processEOS = FALSE;
		}
	}

	if (stream->delayed_stop && !stream->thread_completed)
	{
		DEBUG_DVC("Finishing delayed stream stop, now that eos has processed.");
		tsmf_stream_flush(stream);

		if (stream->decoder->Control)
		{
			stream->decoder->Control(stream->decoder, Control_Flush, NULL);
		}
	}

	if (stream->audio)
	{
		stream->audio->Free(stream->audio);
		stream->audio = NULL;
	}

	SetEvent(stream->stopEvent);

	DEBUG_DVC("out %d", stream->stream_id);

	stream->thread_completed = TRUE;

	return NULL;
}

static void tsmf_stream_start(TSMF_STREAM* stream)
{
	if (!stream)
                return;

	if (!stream->started)
	{
		if (stream->thread_completed)
		{
			stream->thread_completed = FALSE;
			ResetEvent(stream->stopEvent);
			stream->thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE) tsmf_stream_playback_func, stream, CREATE_SUSPENDED, NULL);
		}

		ResumeThread(stream->thread);
		stream->started = TRUE;
	}

	//Sometimes a start is called after a flush, so handle this case like a restart
	else
	{
		if (!stream->decoder)
			return;

		if (stream->decoder->Control)
		{
			stream->decoder->Control(stream->decoder, Control_Restart, NULL);
		}
	}
}

static void tsmf_stream_stop(TSMF_STREAM* stream)
{
	if (!stream)
		return;

	if (!stream->decoder)
		return;

	
	// If stopping after eos - we delay until the eos has been processed
	// this allows us to process any buffers that have been acked even though
	// they have not actually been completely processes by the decoder
	if (stream->eos)
	{
		DEBUG_DVC("Setting up a delayed stop for once the eos has been processed.");
		stream->delayed_stop = 1;
	}

	// Otherwise force stop immediately
	else
	{
		DEBUG_DVC("Stop with no pending eos response, so do it immediately.");
		tsmf_stream_flush(stream);

		if (stream->started && !stream->thread_completed)
		{
			SetEvent(stream->stopEvent);
                	stream->started = FALSE;
		}

		if (stream->decoder->Control)
		{
			stream->decoder->Control(stream->decoder, Control_Flush, NULL);
		}
	}
}

static void tsmf_stream_pause(TSMF_STREAM* stream)
{
	if (!stream)
		return;

	if (!stream->started)
		return;

	if (!stream->decoder)
		return;

	if (stream->decoder->Control)
	{
		stream->decoder->Control(stream->decoder, Control_Pause, NULL);
	}
}

static void tsmf_stream_restart(TSMF_STREAM* stream)
{
	if (!stream)
		return;

	if (!stream->started)
		return;

	if (!stream->decoder)
		return;

	if (stream->decoder->Control)
	{
		stream->decoder->Control(stream->decoder, Control_Restart, NULL);
	}
}

static void tsmf_stream_change_volume(TSMF_STREAM* stream, UINT32 newVolume, UINT32 muted)
{
	if (!stream)
		return;
	
	if (stream->decoder != NULL && stream->decoder->ChangeVolume)
	{
		stream->decoder->ChangeVolume(stream->decoder, newVolume, muted);
	}
	else if (stream->audio != NULL && stream->audio->ChangeVolume)
	{
		stream->audio->ChangeVolume(stream->audio, newVolume, muted);
	}
}

void tsmf_presentation_volume_changed(TSMF_PRESENTATION* presentation, UINT32 newVolume, UINT32 muted)
{
	LIST_ITEM* item;
	TSMF_STREAM* stream;

	presentation->volume = newVolume;
	presentation->muted = muted;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;
		tsmf_stream_change_volume(stream, newVolume, muted);
	}

}

void tsmf_presentation_paused(TSMF_PRESENTATION* presentation)
{
	LIST_ITEM* item;
	TSMF_STREAM* stream;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;
		tsmf_stream_pause(stream);
	}
}

void tsmf_presentation_restarted(TSMF_PRESENTATION* presentation)
{
	LIST_ITEM* item;
	TSMF_STREAM* stream;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;
		tsmf_stream_restart(stream);
	}
}

void tsmf_presentation_start(TSMF_PRESENTATION* presentation)
{
	LIST_ITEM* item;
	TSMF_STREAM* stream;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;
		tsmf_stream_start(stream);
	}
}

void tsmf_presentation_stop(TSMF_PRESENTATION* presentation)
{
	LIST_ITEM* item;
	TSMF_STREAM* stream;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;
		tsmf_stream_stop(stream);
	}
}

void tsmf_presentation_set_geometry_info(TSMF_PRESENTATION* presentation,
	UINT32 x, UINT32 y, UINT32 width, UINT32 height, int num_rects, RDP_RECT* rects)
{
	presentation->output_x = x;
	presentation->output_y = y;
	presentation->output_width = width;
	presentation->output_height = height;

	if (presentation->output_rects)
		free(presentation->output_rects);

	presentation->output_rects = rects;
	presentation->output_num_rects = num_rects;
}

void tsmf_presentation_set_audio_device(TSMF_PRESENTATION* presentation, const char* name, const char* device)
{
	presentation->audio_name = name;
	presentation->audio_device = device;
}

void tsmf_stream_flush(TSMF_STREAM* stream)
{
	//TSMF_SAMPLE* sample;

	/* TODO: free lists */

	if (stream->audio)
		stream->audio->Flush(stream->audio);

	stream->eos = 0;
	stream->eos_message_id = 0;
	stream->eos_channel_callback = NULL;
	stream->delayed_stop = 0;
	stream->last_end_time = 0;
	stream->next_start_time = 0;

	if (stream->major_type == TSMF_MAJOR_TYPE_AUDIO)
	{
		stream->presentation->audio_start_time = 0;
		stream->presentation->audio_end_time = 0;
	}
	else
	{
		tsmf_presentation_restore_last_video_frame(stream->presentation);
		
		if (stream->presentation->last_rects)
		{
			free(stream->presentation->last_rects);
			stream->presentation->last_rects = NULL;
		}

		stream->presentation->last_num_rects = 0;

		if (stream->presentation->output_rects)
		{
			free(stream->presentation->output_rects);
			stream->presentation->output_rects = NULL;
		}

		stream->presentation->output_num_rects = 0;
	}
}

void tsmf_presentation_free(TSMF_PRESENTATION* presentation)
{
	TSMF_STREAM* stream;

	tsmf_presentation_stop(presentation);
	WaitForSingleObject(presentation->mutex, INFINITE);
	list_remove(presentation_list, presentation);
	ReleaseMutex(presentation->mutex);

	while (list_size(presentation->stream_list) > 0)
	{
		stream = (TSMF_STREAM*) list_dequeue(presentation->stream_list);
		tsmf_stream_free(stream);
	}
	list_free(presentation->stream_list);

	CloseHandle(presentation->mutex);

	char filename[4096] = "";
	snprintf(filename, 4096, "/tmp/tsmf-%i.tid", getpid());
	unlink(filename);

	free(presentation);
}

void tsmf_presentation_set_sync(TSMF_PRESENTATION* presentation)
{
	TSMF_STREAM* stream;
	LIST_ITEM* item;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;
		tsmf_stream_set_sync(stream);  
	} 
}

TSMF_STREAM* tsmf_stream_new(TSMF_PRESENTATION* presentation, UINT32 stream_id)
{
	TSMF_STREAM* stream;

	stream = tsmf_stream_find_by_id(presentation, stream_id);

	if (stream)
	{
		DEBUG_WARN("duplicated stream id %d!", stream_id);
		return NULL;
	}

	stream = (TSMF_STREAM*) malloc(sizeof(TSMF_STREAM));
	ZeroMemory(stream, sizeof(TSMF_STREAM));

	stream->eos = 0;
	stream->eos_message_id = 0;
	stream->eos_channel_callback = NULL;

	stream->stream_id = stream_id;
	stream->presentation = presentation;

	stream->started = FALSE;

	stream->stopEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
	stream->thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE) tsmf_stream_playback_func, stream, CREATE_SUSPENDED, NULL);

	stream->sample_list = Queue_New(TRUE, -1, -1);
	stream->sample_list->object.fnObjectFree = free;

	stream->sample_ack_list = Queue_New(TRUE, -1, -1);
	stream->sample_ack_list->object.fnObjectFree = free;

	WaitForSingleObject(presentation->mutex, INFINITE);
	list_enqueue(presentation->stream_list, stream);
	ReleaseMutex(presentation->mutex);

	return stream;
}

TSMF_STREAM* tsmf_stream_find_by_id(TSMF_PRESENTATION* presentation, UINT32 stream_id)
{
	LIST_ITEM* item;
	TSMF_STREAM* stream;

	for (item = presentation->stream_list->head; item; item = item->next)
	{
		stream = (TSMF_STREAM*) item->data;

		if (stream->stream_id == stream_id)
			return stream;
	}

	return NULL;
}

void tsmf_stream_set_format(TSMF_STREAM* stream, const char* name, wStream* s, const char *disabled_codecs)
{
	TS_AM_MEDIA_TYPE mediatype;

	if (stream->decoder)
	{
		DEBUG_WARN("duplicated call");
		return;
	}

	tsmf_codec_parse_media_type(&mediatype, s, disabled_codecs);

	if (mediatype.MajorType == TSMF_MAJOR_TYPE_VIDEO)
	{
		DEBUG_DVC("video width %d height %d bit_rate %d frame_rate %f codec_data %d",
			mediatype.Width, mediatype.Height, mediatype.BitRate,
			(double) mediatype.SamplesPerSecond.Numerator / (double) mediatype.SamplesPerSecond.Denominator,
			mediatype.ExtraDataSize);
	}
	else if (mediatype.MajorType == TSMF_MAJOR_TYPE_AUDIO)
	{
		DEBUG_DVC("audio channel %d sample_rate %d bits_per_sample %d codec_data %d",
			mediatype.Channels, mediatype.SamplesPerSecond.Numerator, mediatype.BitsPerSample,
			mediatype.ExtraDataSize);

		stream->sample_rate = mediatype.SamplesPerSecond.Numerator;
		stream->channels = mediatype.Channels;
		stream->bits_per_sample = mediatype.BitsPerSample;

		if (stream->bits_per_sample == 0)
			stream->bits_per_sample = 16;
	}

	stream->major_type = mediatype.MajorType;
	stream->width = mediatype.Width;
	stream->height = mediatype.Height;
	stream->decoder = tsmf_load_decoder(name, &mediatype);

	// Make sure the presentation object knows that a video stream is present - needed once presentation setup is complete for sync
	if (mediatype.MajorType == TSMF_MAJOR_TYPE_VIDEO)
		stream->presentation->videoStreamPresent = TRUE;

	// Make sure the presentation object knows that a audio stream is present - needed once presentation setup is complete for sync
	if (mediatype.MajorType == TSMF_MAJOR_TYPE_AUDIO)
		stream->presentation->audioStreamPresent = TRUE;

	tsmf_stream_change_volume(stream, stream->presentation->volume, stream->presentation->muted);
}

void tsmf_stream_end(TSMF_STREAM* stream, UINT32 message_id, IWTSVirtualChannelCallback* pChannelCallback)
{
	stream->eos = 1;
	stream->eos_message_id = message_id;
	stream->eos_channel_callback = pChannelCallback;
}

void tsmf_stream_free(TSMF_STREAM* stream)
{
	TSMF_PRESENTATION* presentation = stream->presentation;

	tsmf_stream_stop(stream);

	WaitForSingleObject(presentation->mutex, INFINITE);
	list_remove(presentation->stream_list, stream);
	ReleaseMutex(presentation->mutex);

	Queue_Free(stream->sample_list);
	Queue_Free(stream->sample_ack_list);

	if (stream->decoder)
	{
		stream->decoder->Free(stream->decoder);
		stream->decoder = 0;
	}

	WaitForSingleObject(tsmf_mutex, INFINITE);
	ReleaseMutex(tsmf_mutex);
	CloseHandle(tsmf_mutex);

	SetEvent(stream->thread);

	free(stream);
	stream = 0;
}

void tsmf_stream_set_sync(TSMF_STREAM* stream)
{
	if (!stream->decoder)
		return;
       
	if (stream->decoder->SetStreamSync)
	{
		if ((stream->presentation->videoStreamPresent) &&
		    (stream->presentation->audioStreamPresent))
		{
			stream->decoder->SetStreamSync(stream->decoder, TRUE);
		}
		else
			stream->decoder->SetStreamSync(stream->decoder, FALSE);
	}
}

void tsmf_stream_push_sample(TSMF_STREAM* stream, IWTSVirtualChannelCallback* pChannelCallback,
	UINT32 sample_id, UINT64 start_time, UINT64 end_time, UINT64 duration, UINT32 extensions,
	UINT32 data_size, BYTE* data)
{
	TSMF_SAMPLE* sample;

	WaitForSingleObject(tsmf_mutex, INFINITE);
	
	if (TERMINATING)
	{
		ReleaseMutex(tsmf_mutex);
		return;
	}
	
	ReleaseMutex(tsmf_mutex);

	sample = (TSMF_SAMPLE*) malloc(sizeof(TSMF_SAMPLE));
	ZeroMemory(sample, sizeof(TSMF_SAMPLE));

	sample->sample_id = sample_id;
	sample->start_time = start_time;
	sample->end_time = end_time;
	sample->duration = duration;
	sample->extensions = extensions;
	if ((sample->extensions & 0x00000080) || (sample->extensions & 0x00000040))
		sample->invalidTimestamps = TRUE;
	else
		sample->invalidTimestamps = FALSE;
	sample->stream = stream;
	sample->channel_callback = pChannelCallback;
	sample->data_size = data_size;
	sample->data = malloc(data_size + TSMF_BUFFER_PADDING_SIZE);
	ZeroMemory(sample->data, data_size + TSMF_BUFFER_PADDING_SIZE);
	CopyMemory(sample->data, data, data_size);

	Queue_Enqueue(stream->sample_list, sample);
}

#ifndef _WIN32

static void tsmf_signal_handler(int s)
{
	LIST_ITEM* p_item;
	TSMF_PRESENTATION* presentation;
	LIST_ITEM* s_item;
	TSMF_STREAM* _stream;

	WaitForSingleObject(tsmf_mutex, INFINITE);
	TERMINATING = 1;
	ReleaseMutex(tsmf_mutex);

	if (presentation_list)
	{
		for (p_item = presentation_list->head; p_item; p_item = presentation_list->head)
		{
			presentation = (TSMF_PRESENTATION*) p_item->data;
			tsmf_presentation_free(presentation);
		}
	}

	if (s == SIGINT)
	{
		signal(s, SIG_DFL);
		kill(getpid(), s);
	}
	else if (s == SIGUSR1)
	{
		signal(s, SIG_DFL);
	}
}

#endif

void tsmf_media_init(void)
{
#ifndef _WIN32
	struct sigaction sigtrap;
	sigtrap.sa_handler = tsmf_signal_handler;
	sigemptyset(&sigtrap.sa_mask);
	sigtrap.sa_flags = 0;
	sigaction(SIGINT, &sigtrap, 0);
	sigaction(SIGUSR1, &sigtrap, 0);
#endif

	tsmf_mutex = CreateMutex(NULL, FALSE, NULL);

	if (presentation_list == NULL)
		presentation_list = list_new();
}

