# Water Consumption and Demand Pattern Analytics Using Big Data

## Included
- `app.py`: Streamlit dashboard; attempts to use Apache Spark in local mode.
- `spark_analysis.py`: standalone PySpark analysis script.
- `water_consumption_sample.csv`: 5,000 synthetic demo rows.
- `requirements.txt`: dependencies.

## Data honesty
The included CSV is simulated data for testing only, not official municipal measurements. Do not present its values as real-world findings. Replace it with a sourced real dataset if your instructor requires real observations.

## Run on Windows / VS Code
1. Install Python 3.10 or 3.11.
2. Open this folder in VS Code and open Terminal.
3. Run:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
   If PowerShell blocks activation, use Command Prompt and run `.venv\Scripts\activate.bat`.
4. Check Java with `java -version`. Spark needs a compatible Java/JDK installation.
5. Start dashboard:
   ```powershell
   streamlit run app.py
   ```
6. To run standalone Spark analytics:
   ```powershell
   python spark_analysis.py
   ```

If Spark fails to start, the dashboard will show a warning and use a Pandas fallback so you can inspect the UI. For a proper BDA demonstration, configure Java and ensure the dashboard reports `Apache Spark (local mode)`.

## Dashboard outputs
Regional totals, consumer-category totals, monthly trend, peak month, data filters, preview and CSV export.

## HDFS note
This starter app uses local files and Spark local mode; it does not claim to use HDFS. Add HDFS only if your guide explicitly requires it and there is enough time to configure it.
