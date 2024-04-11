import httpx
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.tasks import task_input_hash

@flow(log_prints=True, persist_result=True)
def fetch_weather(lat: float = 41.87, lon: float = -87.62):
    random_http()
    base_url = "https://api.open-meteo.com/v1/forecast/"
    temps = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    forecasted_temp = float(temps.json()["hourly"]["temperature_2m"][0])
    print(f"Forecasted temp C: {forecasted_temp} degrees")
    report_to_artifact(forecasted_temp)
    # report_to_artifact(19.2)
    return forecasted_temp

@task(cache_key_fn=task_input_hash)
def report_to_artifact(temp):
    print("executing artifact")
    markdown_report = f"""# Weather Report
    
    ## Recent weather

    | Time        | Temperature |
    |:--------------|-------:|
    | Temp Forecast  | {temp} |
    """
    create_markdown_artifact(
        key="weather-report",
        markdown=markdown_report,
        description="Very scientific weather report",
    )

@task(retries = 3)
def random_http():
    random_code = httpx.get("https://httpstat.us/Random/200,500", verify=False)
    if random_code.status_code >= 400:
        raise Exception()
    print(random_code.text)

if __name__ == "__main__":
    fetch_weather.serve(name="deploy-scheduled", cron="* * * * *")