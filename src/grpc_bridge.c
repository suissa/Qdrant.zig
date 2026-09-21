#include <grpc/byte_buffer_reader.h>
#include <grpc/grpc.h>
#include <grpc/grpc_security.h>
#include <grpc/support/alloc.h>
#include <grpc/support/time.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  grpc_channel *channel;
  char *api_key;
} QdrantGrpcClient;

typedef struct {
  unsigned char *response;
  size_t response_len;
  int status_code;
  char *status_message;
} QdrantGrpcResult;

void *qdrant_grpc_client_create(const char *target, const char *api_key,
                                bool use_tls) {
  grpc_init();
  QdrantGrpcClient *client = calloc(1, sizeof(*client));
  if (!client) {
    grpc_shutdown();
    return NULL;
  }
  grpc_channel_credentials *credentials =
      use_tls ? grpc_ssl_credentials_create(NULL, NULL, NULL, NULL)
              : grpc_insecure_credentials_create();
  client->channel = grpc_channel_create(target, credentials, NULL);
  grpc_channel_credentials_release(credentials);
  if (api_key) client->api_key = strdup(api_key);
  if (!client->channel || (api_key && !client->api_key)) {
    if (client->channel) grpc_channel_destroy(client->channel);
    free(client->api_key);
    free(client);
    grpc_shutdown();
    return NULL;
  }
  return client;
}

void qdrant_grpc_client_destroy(void *opaque) {
  QdrantGrpcClient *client = opaque;
  grpc_channel_destroy(client->channel);
  free(client->api_key);
  free(client);
  grpc_shutdown();
}

QdrantGrpcResult qdrant_grpc_unary_call(void *opaque, const char *method,
                                        const unsigned char *request_data,
                                        size_t request_len,
                                        unsigned timeout_ms) {
  QdrantGrpcResult result = {0};
  QdrantGrpcClient *client = opaque;
  grpc_completion_queue *queue = grpc_completion_queue_create_for_next(NULL);
  grpc_slice method_slice = grpc_slice_from_copied_string(method);
  gpr_timespec deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                       gpr_time_from_millis(timeout_ms, GPR_TIMESPAN));
  grpc_call *call = grpc_channel_create_call(client->channel, NULL,
      GRPC_PROPAGATE_DEFAULTS, queue, method_slice, NULL, deadline, NULL);
  grpc_slice_unref(method_slice);

  grpc_slice request_slice = grpc_slice_from_copied_buffer(
      (const char *)request_data, request_len);
  grpc_byte_buffer *request = grpc_raw_byte_buffer_create(&request_slice, 1);
  grpc_slice_unref(request_slice);
  grpc_byte_buffer *response = NULL;
  grpc_metadata_array initial_metadata, trailing_metadata;
  grpc_metadata_array_init(&initial_metadata);
  grpc_metadata_array_init(&trailing_metadata);
  grpc_status_code status = GRPC_STATUS_UNKNOWN;
  grpc_slice status_details = grpc_empty_slice();
  const char *error_string = NULL;
  grpc_metadata metadata = {0};
  if (client->api_key) {
    metadata.key = grpc_slice_from_static_string("api-key");
    metadata.value = grpc_slice_from_copied_string(client->api_key);
  }

  grpc_op operations[6] = {0};
  operations[0].op = GRPC_OP_SEND_INITIAL_METADATA;
  operations[0].data.send_initial_metadata.count = client->api_key ? 1 : 0;
  operations[0].data.send_initial_metadata.metadata = client->api_key ? &metadata : NULL;
  operations[1].op = GRPC_OP_SEND_MESSAGE;
  operations[1].data.send_message.send_message = request;
  operations[2].op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
  operations[3].op = GRPC_OP_RECV_INITIAL_METADATA;
  operations[3].data.recv_initial_metadata.recv_initial_metadata = &initial_metadata;
  operations[4].op = GRPC_OP_RECV_MESSAGE;
  operations[4].data.recv_message.recv_message = &response;
  operations[5].op = GRPC_OP_RECV_STATUS_ON_CLIENT;
  operations[5].data.recv_status_on_client.trailing_metadata = &trailing_metadata;
  operations[5].data.recv_status_on_client.status = &status;
  operations[5].data.recv_status_on_client.status_details = &status_details;
  operations[5].data.recv_status_on_client.error_string = &error_string;

  grpc_call_error start_error = grpc_call_start_batch(
      call, operations, 6, (void *)(uintptr_t)1, NULL);
  if (start_error == GRPC_CALL_OK) {
    grpc_event event = grpc_completion_queue_next(queue, deadline, NULL);
    if (event.type != GRPC_OP_COMPLETE || !event.success)
      status = GRPC_STATUS_UNKNOWN;
  }
  result.status_code = (int)status;
  size_t details_len = GRPC_SLICE_LENGTH(status_details);
  result.status_message = malloc(details_len + 1);
  if (result.status_message) {
    memcpy(result.status_message, GRPC_SLICE_START_PTR(status_details), details_len);
    result.status_message[details_len] = 0;
  }

  if (response) {
    grpc_byte_buffer_reader reader;
    if (grpc_byte_buffer_reader_init(&reader, response)) {
      grpc_slice all = grpc_byte_buffer_reader_readall(&reader);
      result.response_len = GRPC_SLICE_LENGTH(all);
      if (result.response_len) {
        result.response = malloc(result.response_len);
        if (result.response)
          memcpy(result.response, GRPC_SLICE_START_PTR(all), result.response_len);
        else
          result.response_len = 0;
      }
      grpc_slice_unref(all);
      grpc_byte_buffer_reader_destroy(&reader);
    }
  }

  if (client->api_key) grpc_slice_unref(metadata.value);
  if (error_string) gpr_free((void *)error_string);
  grpc_slice_unref(status_details);
  grpc_metadata_array_destroy(&initial_metadata);
  grpc_metadata_array_destroy(&trailing_metadata);
  if (response) grpc_byte_buffer_destroy(response);
  grpc_byte_buffer_destroy(request);
  grpc_call_unref(call);
  grpc_completion_queue_shutdown(queue);
  while (grpc_completion_queue_next(queue, gpr_inf_future(GPR_CLOCK_REALTIME), NULL).type != GRPC_QUEUE_SHUTDOWN) {}
  grpc_completion_queue_destroy(queue);
  return result;
}

void qdrant_grpc_result_destroy(QdrantGrpcResult *result) {
  free(result->response);
  free(result->status_message);
  memset(result, 0, sizeof(*result));
}
