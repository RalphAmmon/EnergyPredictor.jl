module ModelWorker
using DataFrames, ODBC, DBInterface, Preferences, GLM

export my_function, connectDB, fitModels, closeDB, runFitter

function my_function(x)
    # Your function code here
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
            else
                doLoop = false
                println("doLoop = $doLoop")
            end
            DBInterface.execute(conn, "UPDATE ModelQueue SET FitTS = Now() WHERE (ModelQueueID=$modelQueueID);")
        end
        sleep(1.0)
    end
end

function fitModels(conn::DBInterface.Connection, job::DataFrameRow)
    modelConfig = DBInterface.execute(conn, "SELECT * FROM ModelConfig WHERE (ModelConfigID=$(job.ModelConfigID));") |> DataFrame
    modelConfigRow = first(modelConfig)

    xFields = DBInterface.execute(conn, "SELECT Model_X_FieldID, X_Field FROM Model_X_Fields WHERE (ModelConfigID=$(job.ModelConfigID));") |> DataFrame

    trainingData = DBInterface.execute(conn, "SELECT * FROM $(modelConfigRow.TrainingDS)") |> DataFrame

    # groupField = modelConfigRow.GroupField
    weightField = modelConfigRow.WeightField
    groups = groupby(trainingData, :ModelID)

    # modelsDF = DataFrame(groupField => Vector{Int64}(),
    # "LinModel" => Vector{}())

    # modelFormula = term.("EnergyCost") ~ term.(Tuple(["ElectricityPriceBE", "NaturalGasPriceBE", "ElectricityPriceFR", "NaturalGasPriceFR"]))
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
        println(groupID)

        modelVector = fill(groupID, coefSize)
        # myLM = lm(modelFormula, group)
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
    end

    # currentEnergyPrices = DBInterface.execute(conn, "SELECT * FROM CurrentEnergyPrices") |> DataFrame

    # currentEnergyCost = DataFrame(groupField => Vector{Int64}(),
    #     "EnergyCost" => Vector{Float64}())

    # for row in eachrow(modelsDF)

    #     energyCost = predict(row.LinModel, currentEnergyPrices)
    #     push!(currentEnergyCost, (row[groupField], energyCost[1]))
    # end

    # DBInterface.execute(conn, "Delete * FROM CurrentEnergyCost")
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