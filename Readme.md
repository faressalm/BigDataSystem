# Health Care System 
## Goal
The goal of this project is to apply the learned theories and use the learned tools to build a system that can handle massive amounts of data in an efficient, distributed, and reliable manner. There are roughly 3 phases during which we deal with big data:
* Streaming and receiving data
* Persisting and storing data
* Querying and searching data


![hjhjh](https://user-images.githubusercontent.com/58639073/170534351-17cbc3af-0d37-4499-903a-de97a2fc9f3b.png)

### Serving Layer

Trigger Map-Reduce to build Batch
Views periodically. The generation of the Batch Views will be fully controlled by
the scheduler.

### Speed Layer
Speed layer that has an input stream of health messages
and outputs and stores the current analytical results of the required analytics in the
form of Parquet files. The scheduler will trigger running the Spark jobs to
generate the Realtime Views.
Realtime Views that are already consumed in the Serving 
Layer will expired when a day pass and its batch view is created. Which  requires maintaining two sets of the Realtime Views and alternate
between them.


### Backend
The backend is 
simple with only one API exposed: a GET request specifying the window over
which the analytics will be computed. The backend will collect query results by
contacting both speed layer and batch views to aggregate the results and stitch
them together.

### Frontend
The frontend is a simple, single page application that contains a button with two date pickers with
minute precision to define the window ends

![Screenshot from 2022-05-26 18-09-58_2](https://user-images.githubusercontent.com/58639073/170532624-ceaf65b8-dc37-46ee-a902-569a513c0923.png)
