#include "choco.h"

int main(int argc, char ** argv) {
    printf("sizeof shared_ptr: %zu\n", sizeof(shared_ptr<Column>));
    return 0;
}
