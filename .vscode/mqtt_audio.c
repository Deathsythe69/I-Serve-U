#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <mosquitto.h>
#include "cJSON.h"

#define MQTT_HOST "domain-stageingmqttnossl.txninfra.com"
#define MQTT_PORT 1883
#define MQTT_TOPIC "/comp/9876543210"
#define MQTT_CLIENT_ID "nosslstage_client2"
#define MQTT_USERNAME "nosslstage_client2"
#define MQTT_PASSWORD "client@5645##"

#define MAX_FILENAME 128

// Audio Queue Node
typedef struct AudioNode {
    char filename[MAX_FILENAME];
    int priority;
    time_t timestamp;
    struct AudioNode* next;
} AudioNode;

AudioNode* head = NULL;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

// Enqueue Audio File
void enqueue(const char* filename, int priority) {
    pthread_mutex_lock(&queue_mutex);
    
    AudioNode* current = head;
    while (current != NULL) {
        if (strcmp(current->filename, filename) == 0) {
            pthread_mutex_unlock(&queue_mutex);
            return; // Avoid duplicates
        }
        current = current->next;
    }

    AudioNode* newNode = (AudioNode*)malloc(sizeof(AudioNode));
    strncpy(newNode->filename, filename, MAX_FILENAME);
    newNode->priority = priority;
    newNode->timestamp = time(NULL);
    newNode->next = NULL;

    if (!head || priority > head->priority ||
       (priority == head->priority && newNode->timestamp < head->timestamp)) {
        newNode->next = head;
        head = newNode;
    } else {
        AudioNode* temp = head;
        while (temp->next &&
              (temp->next->priority > priority ||
              (temp->next->priority == priority && temp->next->timestamp < newNode->timestamp))) {
            temp = temp->next;
        }
        newNode->next = temp->next;
        temp->next = newNode;
    }

    pthread_mutex_unlock(&queue_mutex);
}

// Dequeue Audio File
AudioNode* dequeue() {
    pthread_mutex_lock(&queue_mutex);
    if (!head) {
        pthread_mutex_unlock(&queue_mutex);
        return NULL;
    }
    AudioNode* node = head;
    head = head->next;
    pthread_mutex_unlock(&queue_mutex);
    return node;
}

// Audio Playback Thread
void* audioPlayer(void* arg) {
    (void)arg;
    while (1) {
        AudioNode* node = dequeue();
        if (node) {
            printf("Playing: %s (Priority: %d)\n", node->filename, node->priority);
            char cmd[512];
            snprintf(cmd, sizeof(cmd), "aplay \"/home/debasis/IserveU/I-Serve-U/audio/%s\"", node->filename);
            system(cmd);
            free(node);
        } else {
            sleep(1);
        }
    }
    return NULL;
}

// MQTT Callback
void onMsg(struct mosquitto* mosq, void* obj, const struct mosquitto_message* message) {
    char* payload = (char*)message->payload;
    payload[message->payloadlen] = '\0';
    printf("Message received: %s\n", payload);

    cJSON* json = cJSON_Parse(payload);
    if (json) {
        cJSON* file = cJSON_GetObjectItem(json, "file");
        cJSON* priority = cJSON_GetObjectItem(json, "priority");
        if (cJSON_IsString(file) && cJSON_IsNumber(priority)) {
            enqueue(file->valuestring, priority->valueint);
        }
        cJSON_Delete(json);
    }
}

int main() {
    mosquitto_lib_init();

    struct mosquitto* mosq = mosquitto_new(MQTT_CLIENT_ID, true, NULL);
    if (!mosq) {
        fprintf(stderr, "Failed to create Mosquitto client\n");
        return 1;
    }

    mosquitto_username_pw_set(mosq, MQTT_USERNAME, MQTT_PASSWORD);
    mosquitto_message_callback_set(mosq, onMsg);

    if (mosquitto_connect(mosq, MQTT_HOST, MQTT_PORT, 60)) {
        fprintf(stderr, "Unable to connect to broker.\n");
        return 1;
    }

    mosquitto_subscribe(mosq, NULL, MQTT_TOPIC, 1);
    printf("Subscribed to topic: %s\n", MQTT_TOPIC);

    pthread_t playerThread;
    pthread_create(&playerThread, NULL, audioPlayer, NULL);

    mosquitto_loop_start(mosq);

    while (1) {
        sleep(10);
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();

    return 0;
}
