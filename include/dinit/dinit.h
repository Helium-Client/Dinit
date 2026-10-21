#ifndef DINIT_DINIT_H
#define DINIT_DINIT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t HeliumStatus;
#define HELIUM_OK ((HeliumStatus)0)
#define HELIUM_ERROR ((HeliumStatus)-1)
#define HELIUM_INCOMPATIBLE_ABI ((HeliumStatus)-2)
#define HELIUM_NOT_FOUND ((HeliumStatus)-3)
#define HELIUM_INVALID_ARGUMENT ((HeliumStatus)-4)

typedef struct HeliumServiceHeaderV1 {
    uint32_t struct_size;
    uint32_t version;
} HeliumServiceHeaderV1;

#define HELIUM_DINIT_SERVICE_ID "helium.dinit.events"
#define HELIUM_DINIT_SERVICE_VERSION 1u

typedef enum HeliumDinitEventTypeV1 {
    HELIUM_DINIT_CLIENT_STARTED = 1,
    HELIUM_DINIT_CLIENT_STOPPING = 2,
    HELIUM_DINIT_TICK_PRE = 10,
    HELIUM_DINIT_TICK_POST = 11,
    HELIUM_DINIT_RENDER_FRAME = 20
} HeliumDinitEventTypeV1;

#define HELIUM_DINIT_EVENT_CANCELLED 1u

typedef struct HeliumDinitEventV1 {
    uint32_t struct_size;
    uint32_t event_type;
    uint32_t flags;
    uint32_t reserved;
    uint64_t sequence;
    uint64_t tick;
    float delta_seconds;
} HeliumDinitEventV1;

typedef HeliumStatus (*HeliumDinitEventCallbackV1)(
    void *user_context,
    HeliumDinitEventV1 *event
);
typedef HeliumStatus (*HeliumDinitSubscribeFnV1)(
    void *service_context,
    uint32_t event_type,
    int32_t priority,
    HeliumDinitEventCallbackV1 callback,
    void *user_context,
    uint64_t *out_subscription_id
);
typedef HeliumStatus (*HeliumDinitUnsubscribeFnV1)(
    void *service_context,
    uint64_t subscription_id
);

typedef struct HeliumDinitServiceV1 {
    HeliumServiceHeaderV1 header;
    void *context;
    HeliumDinitSubscribeFnV1 subscribe;
    HeliumDinitUnsubscribeFnV1 unsubscribe;
} HeliumDinitServiceV1;

#ifdef __cplusplus
}
#endif

#endif
