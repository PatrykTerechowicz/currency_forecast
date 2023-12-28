# Currency Prediction
- Uses NBP API to fetch currency history data.
- Fetches data daily/weekly and updates our data target.
- We save data already ready for ML algorithm.
- Uses PySpark ML to build model after every fetch.
- Saves models and makes predictions.
- Predictions will be visible on Web UI that uses Flask or Dash

After I create this tool that works with flat-files (saves data on disk),
I'll swap project into Glue.