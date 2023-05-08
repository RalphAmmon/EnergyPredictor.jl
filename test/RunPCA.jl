using MultivariateStats, CSV, DataFrames, ModelWorker, DBInterface

conn = ModelWorker.connectDB()

modelWorkQueue = DBInterface.execute(conn, "SELECT * FROM ModelWorkQueue") |> DataFrame
job = first(modelWorkQueue)
pcaConfig = DBInterface.execute(conn, "SELECT * FROM PCA_Config WHERE (PCA_ConfigID=$(job.PCA_ConfigID));") |> DataFrame
configRow = first(pcaConfig)
configID = configRow.PCA_ConfigID
trainingHorizon = configRow.TrainingHorizonID
DBInterface.execute(conn, "UPDATE CurrentTrainingHorizon SET TrainingHorizonID = $(configRow.TrainingHorizonID)")
predictionHorizon = configRow.PredictionHorizonID
DBInterface.execute(conn, "UPDATE CurrentPredictionHorizon SET PredictionHorizonID = $(configRow.PredictionHorizonID)")


# load Tariffs
tariffs = DBInterface.execute(conn, "SELECT * FROM TariffsKT") |> DataFrame

# traning set tariffs
xTariffs = convert(Matrix{Float64}, Matrix(tariffs[:, 2:end])')

# train a PCA tariff model, allowing up to 3 dimensions
pcaModel = fit(PCA, xTariffs; maxoutdim=3)

# predict principle components
tariffs2Predict = DBInterface.execute(conn, "SELECT * FROM Tariffs2PredictKT") |> DataFrame
pTariffs = convert(Matrix{Float64}, Matrix(tariffs2Predict[:, 2:end])')
tariffsPCA = DataFrame(hcat(tariffs.PeriodID, predict(pcaModel, pTariffs)'), [:PeriodID, :PC1, :PC2, :PC3])


DBInterface.execute(conn, "Delete * FROM TariffsPCA WHERE (PCA_ConfigID=$configID)")

for row in eachrow(tariffsPCA)
    DBInterface.execute(conn, "INSERT INTO TariffsPCA ( PCA_ConfigID, PeriodID, PC1, PC2, PC3) values($configID, $(row.PeriodID), $(row.PC1), $(row.PC2), $(row.PC3))")
end

ModelWorker.closeDB(conn)
