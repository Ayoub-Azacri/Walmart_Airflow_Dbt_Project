from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
import pendulum

# Schedule the DAG to run every day at 11 AM Paris time
@dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False, # True if you want to run the DAG for the days you missed, False if you only want to run it for the current day
        start_date=pendulum.datetime(year=2026, month=9, day=4, tz="Europe/Paris")
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host="your_databricks_host",
            token ="your_databricks_token"
        )

        # trigger the job
        job_trigger = ws.jobs.run_now(job_id="your_databricks_job_id")

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR
            ]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
            time.sleep(5)

        return "CDC ingestion completed."

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target/ && rm -rf /opt/airflow/walmart_project/logs/"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness --profiles-dir ."

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_t --profiles-dir ."
    )

    silver_technican_test = BashOperator(
        task_id="silver_technican_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_t --profiles-dir ."
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_b --profiles-dir ."
    )

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_b --profiles-dir ."
    )

    ## build ephemeral models
    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/ephemeral --profiles-dir ."
    )

    ## create dimensions via snapshot models
    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt snapshot --profiles-dir ."
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/fact --profiles-dir ."
    )

    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> silver_technical
        >> silver_technican_test
        >> silver_business
        >> silver_business_test
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_facts
    )

orchestrate_dag = orchestrate()
