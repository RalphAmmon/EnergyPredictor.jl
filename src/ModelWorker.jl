module ModelWorker
using DataFrames, ODBC, DBInterface, Preferences, MultivariateStats, GLM

export my_function, connectDB, fitModels, closeDB, runFitter, doPCA

function my_function(x)
    return 2 * x
end

function connectDB()
    uuid = Base.UUID("28d85817-24a3-440c-8918-5e5faabb4cd2")
    accdbFilename = load_preference(uuid, "accdbFileName")
    localDataPath = load_preference(uuid, "localDataPath")

    cnxn_str = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq="
    db = joinpath(localDataPath, accdbFilename)
    dsin = "$cnxn_str$db"
    return ODBC.Connection(dsin)
end

function runFitter(conn::DBInterface.Connection)
    doLoop = true
    while doLoop
        modelWorkQueue = DBInterface.execute(conn, "SELECT * FROM ModelWorkQueue") |> DataFrame
        for job in eachrow(modelWorkQueue)
            modelQueueID = job.ModelQueueID
            if job.Task == "Fit"
                println("Fit $modelQueueID")
                fitModels(conn, job)
            elseif job.Task == "PCA"
                println("doPCA")
                doPCA(conn, job)
            else
                doLoop = false
                println("doLoop = $doLoop")
            end
            DBInterface.execute(conn, "UPDATE ModelQueue SET FitTS = Now() WHERE (ModelQueueID=$modelQueueID);")
        end
        sleep(1.0)
    end
end
function doPCA(conn::DBInterface.Connection, job::DataFrameRow)
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
end

function fitModels(conn::DBInterface.Connection, job::DataFrameRow)
    modelConfig = DBInterface.execute(conn, "SELECT * FROM ModelConfig WHERE (ModelConfigID=$(job.ModelConfigID));") |> DataFrame
    modelConfigRow = first(modelConfig)

    xFields = DBInterface.execute(conn, "SELECT Model_X_FieldID, X_Field FROM Model_X_Fields WHERE (ModelConfigID=$(job.ModelConfigID));") |> DataFrame

    trainingData = DBInterface.execute(conn, "SELECT * FROM $(modelConfigRow.TrainingDS)") |> DataFrame

    weightField = modelConfigRow.WeightField
    groups = groupby(trainingData, :ModelID)

    modelFormula = term.(modelConfigRow.Y_Field) ~ term.(Tuple(xFields.X_Field))


    modelCoefficients = DataFrame(
        ModelID=Vector{Int64}(),
        Model_X_FieldID=Vector{Int64}(),
        Coef=Vector{Float64}(),
        StdError=Vector{Float64}(),
        t=Vector{Float64}(),
        PrGtAbsT=Vector{Float64}(),
        Lower95Percent=Vector{Float64}(),
        Upper95Percent=Vector{Float64}()
    )

    model_X_FieldID = vcat([1], xFields.Model_X_FieldID)
    coefSize = size(model_X_FieldID)[1]

    for group in groups
        groupID = group.ModelID[1]
        # println(groupID)

        modelVector = fill(groupID, coefSize)
        myLM = glm(modelFormula, group, Normal(), IdentityLink(), wts=convert(Vector{Float64}, group[:, weightField]))
        coefTbl = coeftable(myLM)

        modelCoefficients = vcat(modelCoefficients,
            DataFrame(
                ModelID=modelVector,
                Model_X_FieldID=model_X_FieldID,
                Coef=coefTbl.cols[1],
                StdError=coefTbl.cols[2],
                t=coefTbl.cols[3],
                PrGtAbsT=coefTbl.cols[4],
                Lower95Percent=coefTbl.cols[5],
                Upper95Percent=coefTbl.cols[6]
            )
        )

        DBInterface.execute(conn,
            "UPDATE Models SET Deviance = $(deviance(myLM)), ModelFit = $(myLM.model.fit), FitTS = Now() WHERE (ModelID=$groupID);"
        )
    end

    DBInterface.execute(conn,
        """DELETE ModelCoefficients.* 
        FROM Models INNER JOIN ModelCoefficients ON Models.ModelID = ModelCoefficients.ModelID 
        WHERE (Models.ModelConfigID=$(modelConfig.ModelConfigID[1]));"""
    )

    for row in eachrow(modelCoefficients)
        DBInterface.execute(conn,
            """INSERT INTO ModelCoefficients ( ModelID, Model_X_FieldID, Coef, StdError, t, PrGtAbsT, Lower95Percent, Upper95Percent ) 
            values( 
                $(row.ModelID), 
                $(row.Model_X_FieldID),
                $(row.Coef),
                $(row.StdError),
                $(row.t),
                $(row.PrGtAbsT),
                $(row.Lower95Percent),
                $(row.Upper95Percent)
            )"""
        )
    end
end

function closeDB(conn::DBInterface.Connection)
    DBInterface.close!(conn)
end
end