#include <dinit/dinit.h>
#include <stddef.h>

_Static_assert(sizeof(HeliumStatus) == 4, "status must be 32-bit");
_Static_assert(sizeof(HeliumServiceHeaderV1) == 8, "service header layout changed");
_Static_assert(sizeof(HeliumDinitEventV1) == 40, "Dinit event layout changed");
_Static_assert(offsetof(HeliumDinitServiceV1, header) == 0, "header must be first");

int main(void) {
    HeliumDinitEventV1 event = {0};
    return event.event_type == 0 && HELIUM_DINIT_SERVICE_VERSION == 1u ? 0 : 1;
}
