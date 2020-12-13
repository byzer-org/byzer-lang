#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

const int NUM_CPU_STATES = 10;

enum CPUStates
{
    S_USER = 0,
    S_NICE,
    S_SYSTEM,
    S_IDLE,
    S_IOWAIT,
    S_IRQ,
    S_SOFTIRQ,
    S_STEAL,
    S_GUEST,
    S_GUEST_NICE
};

typedef struct CPUData
{
    std::string cpu;
    size_t times[NUM_CPU_STATES];
} CPUData;

void ReadStatsCPU(std::vector<CPUData> & entries);

size_t GetIdleTime(const CPUData & e);
size_t GetActiveTime(const CPUData & e);

float CalculateStats(const std::vector<CPUData> & entries1, const std::vector<CPUData> & entries2);

float GetCPULoad()
{
    std::vector<CPUData> entries1;
    std::vector<CPUData> entries2;

    // snapshot 1
    ReadStatsCPU(entries1);

    // 100ms pause
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // snapshot 2
    ReadStatsCPU(entries2);

    // print output

    return CalculateStats(entries1, entries2);
}

void ReadStatsCPU(std::vector<CPUData> & entries)
{
    std::ifstream fileStat("/proc/stat");

    std::string line;

    const std::string STR_CPU("cpu");
    const std::size_t LEN_STR_CPU = STR_CPU.size();
    const std::string STR_TOT("tot");

    while(std::getline(fileStat, line))
    {
        // cpu stats line found
        if(!line.compare(0, LEN_STR_CPU, STR_CPU))
        {
            std::istringstream ss(line);

            // store entry
            entries.emplace_back(CPUData());
            CPUData & entry = entries.back();

            // read cpu label
            ss >> entry.cpu;

            // remove "cpu" from the label when it's a processor number
            if(entry.cpu.size() > LEN_STR_CPU)
                entry.cpu.erase(0, LEN_STR_CPU);
                // replace "cpu" with "tot" when it's total values
            else
                entry.cpu = STR_TOT;

            // read times
            for(int i = 0; i < NUM_CPU_STATES; ++i)
                ss >> entry.times[i];
        }
    }
}

size_t GetIdleTime(const CPUData & e)
{
    return	e.times[S_IDLE] +
              e.times[S_IOWAIT];
}

size_t GetActiveTime(const CPUData & e)
{
    return	e.times[S_USER] +
              e.times[S_NICE] +
              e.times[S_SYSTEM] +
              e.times[S_IRQ] +
              e.times[S_SOFTIRQ] +
              e.times[S_STEAL] +
              e.times[S_GUEST] +
              e.times[S_GUEST_NICE];
}

float CalculateStats(const std::vector<CPUData> & entries1, const std::vector<CPUData> & entries2)
{
    const size_t NUM_ENTRIES = entries1.size();
    auto wow = 0.0;

    for(size_t i = 0; i < NUM_ENTRIES; ++i)
    {
        const CPUData & e1 = entries1[i];
        const CPUData & e2 = entries2[i];

        const float ACTIVE_TIME	= static_cast<float>(GetActiveTime(e2) - GetActiveTime(e1));
        const float IDLE_TIME	= static_cast<float>(GetIdleTime(e2) - GetIdleTime(e1));
        const float TOTAL_TIME	= ACTIVE_TIME + IDLE_TIME;
        wow += ACTIVE_TIME/TOTAL_TIME;
    }
    return wow/NUM_ENTRIES;
}