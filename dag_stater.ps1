# Define Airflow commands
$webserverCmd = "airflow webserver"
$schedulerCmd = "airflow scheduler"

# Start the Airflow webserver in the background
Start-Job -ScriptBlock {
    airflow webserver
} | Out-Null

# Start the Airflow scheduler in the background
Start-Job -ScriptBlock {
    airflow scheduler
} | Out-Null

Write-Host "Airflow webserver and scheduler are running in the background. Use Get-Job to check their status."
