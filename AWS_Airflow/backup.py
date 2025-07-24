

@task
def store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY users FROM STDIN WITH CSV HEADER",
        filename="/tmp/user_info.csv"
    )

return[process_user(extract_user(create_table >> is_api_available())) >> store_user()]
user_processing()