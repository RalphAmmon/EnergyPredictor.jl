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
    end
end

function fitModels(conn::DBInterface.Connection, job::DataFrameRow)
    modelConfig = DBInterface.execute(conn, "SELECT * FROM ModelConfig WHERE (ModelConfigID=$(job.ModelConfigID));") |> DataFrame
    modelConfigRow = first(modelConfig)

    xFields = DBInterface.execute(conn, "SELECT X_Field FROM Model_X_Fields WHERE (ModelConfigID=$(job.ModelConfigID));") |> DataFrame

    trainingData = DBInterface.execute(conn, "SELECT * FROM $(modelConfigRow.TrainingDS)") |> DataFrame

    groupField = modelConfigRow.GroupField
    groups = groupby(trainingData, Symbol(groupField))

    modelsDF = DataFrame(groupField => Vector{Int64}(),
        "LinModel" => Vector{}())

    # modelFormula = term.("EnergyCost") ~ term.(Tuple(["ElectricityPriceBE", "NaturalGasPriceBE", "ElectricityPriceFR", "NaturalGasPriceFR"]))
    modelFormula = term.(modelConfigRow.Y_Field) ~ term.(Tuple(xFields.X_Field))

    for group in groups
        groupID = group[1, groupField]
        println(groupID)
        push!(modelsDF, (groupID, lm(modelFormula, group)))
    end

    currentEnergyPrices = DBInterface.execute(conn, "SELECT * FROM CurrentEnergyPrices") |> DataFrame

    currentEnergyCost = DataFrame(groupField => Vector{Int64}(),
        "EnergyCost" => Vector{Float64}())

    for row in eachrow(modelsDF)
        energyCost = predict(row.LinModel, currentEnergyPrices)
        push!(currentEnergyCost, (row[groupField], energyCost[1]))
    end

    DBInterface.execute(conn, "Delete * FROM CurrentEnergyCost")

    for row in eachrow(currentEnergyCost)
        DBInterface.execute(conn, "INSERT INTO CurrentEnergyCost ( $groupField, EnergyCost ) values( $(row.FinishedGoodID) , $(row.EnergyCost))")
    end
end

function closeDB(conn::DBInterface.Connection)
    DBInterface.close!(conn)
end
end