#include <iostream>
#ifdef __linux__
#include "common/linuxsys.h"
#else
#include "common/macossys.h"
#endif

int main() {
    std::cout << GetCPULoad() << std::endl;
    return 0;
}
