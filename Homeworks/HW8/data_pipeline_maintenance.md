## Data Engineering Team
1. Kshama Kumar
2. Aruna Sharma
3. Biswa Kalyan Rath
4. Disha Kapoor

## Pipelines
1. Profit
    a. Unit-level profit needed for experiments
    b. Aggregate profit reported to investors
2. Growth
    a. Aggregate growth reported to investors
    b. Daily growth needed for experiments
3. Engagement
    a. Aggregate engagement reported to investors

## On-call Schedule
1. Every engineer will be on-call on a weekly rotation schedule.
2. Night time, weekend and Holiday on-call will be partial and only if necessary.
3. Necessary situations include:
    - Making sure daily data has been populated correctly.
    - Resolving/finding a workaround for P1 errors.
4. During holiday periods, the rotation will happen every two days based on the engineers availability.
## Runbooks for each pipeline 

### Aggregate profit reported to investors
#### Owners
Primary: Kshama Kumar
Secondary: Aruna Sharma

#### Common Issues
1. There could be missing unit-level profit from the previous days. 
    In this case, you could use the weekly average value to fill up the empty field and continue.
2. Data compatibility issues that could lead to aggregation errors.
    This could be due to an issue with a different pipeline. Document the bug and give an estimate for the fix. 

#### SLA
1. Unit level data should be ready by 7pm PST for the aggregate pipeline to run.


### Aggregate growth reported to investors
#### Owners
Primary: Biswa Kalyan Rath
Secondary: Disha Kapoor

#### Common Issues
1. Sometimes queries could slow down if it is aggregating a lot of data.
  - Figure out the blocking areas in the code and set an estimate for the optimization.
2. Permission issues while accessing the pipelines.
  - Check with available IT team for access asap.

#### SLA
1. Make sure that the charts in the report are correctly showing up. 
 
### Aggregate engagement reported to investors
#### Owners
Primary: Kshama Kumar
Secondary: Biswa Kalyan Rath

#### Common Issues
1. Data is not deduped correctly.
    - Conduct a quick root cause analysis and document the issue.
2. The timestamps are inconsistent between the client systems and the server
    - Use the existing script to update the timestamps into UTC timezone.

#### SLA
1. Once the report is ready, send an email to the analyst.

