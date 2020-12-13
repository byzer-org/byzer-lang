#pragma once

float GetCPULoad();
int CalculateCPULoad(unsigned long * pulSystem, unsigned long * pulUser, unsigned long * pulNice, unsigned long * pulIdle);
