#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h> //POSIX Threads is an execution model that exists independently from a programming language
#include <mosquitto.h>
#include <json-c/json.h>

#define AUDIO_DIR "/home/debasis/IserveU/I-Serve-U/audio"
#define BROKER_HOST "stagingmqttnossl.txninfra.com"
#define BROKER_PORT 1883
#define MQTT_USER "nosslstage_client2"
#define MQTT_PASS "client@5645##"
#define MQTT_TOPIC "/comp/7008348993"

typedef struct Track {
    char name[256];
    int rank;
    time_t arrival;
    struct Track *next;
} Track;

Track *queue = NULL;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

int already_queued(const char *name) {
    for (Track *t = queue; t; t = t->next) {
        if (strcmp(t->name, name) == 0) return 1;
    }
    return 0;
}

void push_track(const char *name, int rank) {
    if (already_queued(name)) {
        printf("[!] Skipping duplicate: %s\n", name);
        return;
    }

    Track *new = malloc(sizeof(Track));
    strncpy(new->name, name, sizeof(new->name) - 1);
    new->rank = rank;
    new->arrival = time(NULL);
    new->next = NULL;

    pthread_mutex_lock(&lock);

    if (!queue || rank > queue->rank || 
        (rank == queue->rank && new->arrival < queue->arrival)) {
        new->next = queue;
        queue = new;
    } else {
        Track *cur = queue;
        while (cur->next && (
            cur->next->rank > rank ||
            (cur->next->rank == rank && cur->next->arrival <= new->arrival))) {
            cur = cur->next;
        }
        new->next = cur->next;
        cur->next = new;
    }

    printf("[+] Queued: %s (priority %d)\n", name, rank);
    pthread_mutex_unlock(&lock);
}

Track *pop_track() {
    pthread_mutex_lock(&lock);
    if (!queue) {
        pthread_mutex_unlock(&lock);
        return NULL;
    }
    Track *out = queue;
    queue = queue->next;
    pthread_mutex_unlock(&lock);
    return out;
}

void *dj_worker(void *arg) {
    (void)arg;
    while (1) {
        Track *play = pop_track();
        if (play) {
            char filepath[512];
            snprintf(filepath, sizeof(filepath), "%s/%s", AUDIO_DIR, play->name);

            if (access(filepath, F_OK) != 0) {
                printf("[-] File missing: %s\n", filepath);
                free(play);
                continue;
            }

            printf("[\U0001F3B5] Now playing: %s (priority %d)\n", play->name, play->rank);
            char command[600];
            snprintf(command, sizeof(command), "aplay \"%s\" > /dev/null 2>&1", filepath);
            system(command);
            free(play);
        } else {
            sleep(1);
        }
    }
    return NULL;
}

void handle_mqtt(struct mosquitto *mq, void *ctx, const struct mosquitto_message *msg) {
    (void)mq; (void)ctx;

    struct json_object *payload = json_tokener_parse(msg->payload);
    if (!payload) {
        printf("[x] Invalid JSON\n");
        return;
    }

    struct json_object *file_obj = NULL, *prio_obj = NULL;
    if (!json_object_object_get_ex(payload, "file", &file_obj) ||
        !json_object_object_get_ex(payload, "priority", &prio_obj)) {
        printf("[x] Missing fields\n");
        json_object_put(payload);
        return;
    }

    const char *track = json_object_get_string(file_obj);
    int prio = json_object_get_int(prio_obj);

    push_track(track, prio);
    json_object_put(payload);
}

int main() {
    mosquitto_lib_init();
    struct mosquitto *client = mosquitto_new(NULL, true, NULL);
    if (!client) {
        fprintf(stderr, "!! MQTT init failed\n");
        return 1;
    }

    mosquitto_username_pw_set(client, MQTT_USER, MQTT_PASS);
    mosquitto_tls_set(client, NULL, NULL, NULL, NULL, NULL);
    mosquitto_message_callback_set(client, handle_mqtt);

    if (mosquitto_connect(client, BROKER_HOST, BROKER_PORT, 60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "!! Broker connect error\n");
        return 1;
    }

    mosquitto_subscribe(client, NULL, MQTT_TOPIC, 0);
    printf("[\u2713] Listening on topic: %s\n", MQTT_TOPIC);

    pthread_t dj_thread;
    pthread_create(&dj_thread, NULL, dj_worker, NULL);

    mosquitto_loop_forever(client, -1, 1);

    mosquitto_destroy(client);
    mosquitto_lib_cleanup();
    return 0;
}
