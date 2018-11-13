## Versioning Policy


Starting with MLSQL 1.1.3, the MLSQL project will follow the semantic versioning guidelines with a few deviations.
These small differences account for MLSQL’s nature as a multi-module project.


## MLSQL Versions
Each MLSQL release will be versioned: \[MAJOR\].\[FEATURE\].\[MAINTENANCE\]

* MAJOR: All releases with the same major version number will have API compatibility. Major version numbers
         will remain stable over long periods of time. For instance, 1.X.Y may last 1 year or more.
* FEATURE: Feature releases will typically contain new features, improvements, and bug fixes.
           Each feature release will have a merge window where new patches can be merged,
           a QA window when only fixes can be merged, then a final period where voting occurs on release candidates.
           These windows will be announced immediately after the previous feature release to give people plenty of time,
           and over time, we might make the whole release process more regular (similar to Ubuntu).
* MAINTENANCE: Maintenance releases will occur more frequently and depend
               on specific patches introduced (e.g. bug fixes) and their urgency.
               In general these releases are designed to patch bugs. However, higher level libraries may introduce small features,
               such as a new algorithm, provided they are entirely additive and isolated from existing code paths.
               MLSQL core may not introduce any features.


## Release Cadence
In general, feature (“minor”) releases occur about every 1.5 months.
Hence, MLSQL 1.2.x would generally be released about 1.5 months after 1.1.x.
Maintenance releases happen as needed in between feature releases.
Major releases do not happen according to a fixed schedule.

Every two weeks, we will release maintenance version. BugFix will be introduced in maintenance version.


MLSQL 1.2.0 Release Window

|Date|	 	Event|
|-----|-------|
|Mid Dec 2018    |	 	Code freeze. Release branch cut.  |
|Late Dec 2018   |	 	QA period. Focus on bug fixes, tests, stability and docs. Generally, no new features merged.|
|Early Jan 2019	 | 	    Release candidates (RC), voting, etc. until final release passes|



