# Week 5 Data Pipeline Maintenance

### Homework

Imagine you're in a group of 4 data engineers, you will be in charge of creating the following things:

You are in charge of managing these 5 pipelines that cover the following business areas:
 
- Profit
 - Unit-level profit needed for experiments
 - Aggregate profit reported to investors
- Growth
 - Aggregate growth reported to investors
 - Daily growth needed for experiments
- Engagement 
 - Aggregate engagement reported to investors

You are in charge of figuring out the following things:

- Who is the primary and secondary owners of these pipelines?
- What is an on-call schedule that is fair
  - Think about holidays too!
- Creating run books for all pipelines that report metrics to investors
  - What could potentially go wrong in these pipelines?
  - (you don't have to recommend a course of action though since this is an imaginatione exercise)
  
Create a markdown file submit it!


## Pipeline ownership

| Area       | Pipeline                                      | Primary Owner | Secondary Owner     |
|------------|-----------------------------------------------|---------------|---------------------|
| Profit     | Unit-level profit needed for experiments      | Eng1          | Eng2 / Eng3         |
| Profit     | Aggregate profit reported to investors        | Eng2          | Eng4 / Eng5         |
| Growth     | Aggregate growth reported to investors        | Eng3          | Eng1 / Eng5         |
| Growth     | Daily growth needed for experiments           | Eng4          | Eng1 / Eng2         |
| Engagement | Aggregate engagement reported to investors    | Eng5          | Eng3 / Eng4         |


## On call schedule

Weekly Rotation: Each engineer takes 1 week at a time.
No Consecutive Weeks: No two weeks in a row assigned
Holiday Load Balancing: Holidays spreaded evenly, yearly rotations
Visibility: Publish schedule at least a quarter in advance.

| Holiday Season | 2025 Coverage | 2026 Coverage | 2027 Coverage |
| -------------- | ------------- | ------------- | ------------- |
| Christmas      | Eng1          | Eng2          | Eng3          |
| New Year       | Eng2          | Eng3          | Eng4          |
| Thanksgiving   | Eng3          | Eng4          | Eng5          |
| July 4th       | Eng4          | Eng5          | Eng1          |
| Labor Day      | Eng5          | Eng1          | Eng2          |


## Runbooks for investor-related pipelines


- Common issues in the pipelines and datasets for investors
  - Problems with upstream pipelines and datasets, for example: data not updated, introductions of nulls or foreign values into the models, business logic changes not reflected in the model, integration problems
  - Problems with downstream pipelines and datasets, for example: problems with values in dahsboareds, data not appearing in visualization tools, duplicated values



