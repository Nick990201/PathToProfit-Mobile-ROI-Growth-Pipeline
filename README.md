
The Origin Story
While working on an upcoming mobile app launch, I started asking myself some critical business questions that every startup faces:
* **How ​​exactly will they make money?**
* **What is their marketing strategy and at what cost?**
* **How ​​long will it take them to break even?**
* **How ​​much daily engagement do they need from their users to not go into the red?**

Instead of guessing, I decided to build a **data engineering pipeline** to answer these questions with hard numbers.

### Project Idea:
I designed this platform to serve two purposes:
1. **Phase 1 (Simulation):** Before the official launch, the system runs a simulation to highlight potential risks and identify the “golden threshold” of user engagement.

2. **Phase 2 (Production):** Once live, the architecture is designed to switch from simulated data to real-time API feeds (Facebook, Google, App Store), refreshing daily to provide management with constant updates on our ROI.

### The Technology Stack
To do this, I used a modern data stack:
* **Apache Airflow:** To orchestrate tasks and ensure data is refreshed like clockwork.
* **PostgreSQL:** Our central data store where all user activity and marketing spend is stored.
* **Docker & Docker Compose:** To keep the entire infrastructure portable and easy to deploy.
* **Python (Pandas/SQLAlchemy):** For the heavy lifting - calculating the math behind the revenue.

### What I Learned (Analysis)
The pipeline generates several key reports. The most important is the **Path to Profitability** chart. It shows that the success of their app is highly tied to daily engagement.
* **Key takeaway:** If users spend less than 10 minutes a day, their marketing spend (CAC) will eat into their budget. If they hit 20+ minutes, they reach breakeven much faster.

### Challenges
It wasn’t all smooth sailing. Setting up a **Docker network** so that Airflow could “talk” to the Postgres database was a hurdle. I also spent a few hours debugging why my PostgreSQL driver wasn’t loading into the Airflow container, which taught me a lot about how Docker handles dependencies and environment isolation.

---
*Note: This project is currently using simulated data for demonstration purposes, but is designed to be easily connected to a real-world marketing API.*
