using DataFrames, ODBC, DBInterface, Preferences, GLM

uuid = Base.UUID("28d85817-24a3-440c-8918-5e5faabb4cd2")
accdbFilename = load_preference(uuid, "accdbFileName")
localDataPath = load_preference(uuid, "localDataPath")

cnxn_str = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq="
db = joinpath(localDataPath, accdbFilename) 
# db = "data/CostModel.accdb"
dsin = "$cnxn_str$db"

conn = ODBC.Connection(dsin)
trainingData = DBInterface.execute(conn, "SELECT * FROM TrainingData") |> DataFrame

groups = groupby(trainingData, :FinishedGoodID)

modelsDF = DataFrame(FinishedGoodID=Vector{Int64}(),
    LinModel=Vector{}())

modelFormula = term.("EnergyCost") ~ term.(Tuple(["ElectricityPriceBE", "NaturalGasPriceBE", "ElectricityPriceFR", "NaturalGasPriceFR"]))

for group in groups
    println(group.FinishedGoodID[1])
    # push!(modelsDF, (FinishedGoodID=group.FinishedGoodID[1], LinModel=lm(@formula(EnergyCost ~ ElectricityPriceBE + NaturalGasPriceBE + ElectricityPriceFR + NaturalGasPriceFR), group)))
    push!(modelsDF, (FinishedGoodID=group.FinishedGoodID[1], LinModel=lm(modelFormula, group)))
end

currentEnergyPrices = DBInterface.execute(conn, "SELECT * FROM CurrentEnergyPrices") |> DataFrame

currentEnergyCost = DataFrame(FinishedGoodID=Vector{Int64}(),
    EnergyCost=Vector{Float64}())

for row in eachrow(modelsDF)
    energyCost = predict(row.LinModel, currentEnergyPrices)
    push!(currentEnergyCost, (FinishedGoodID=row.FinishedGoodID, EnergyCost=energyCost[1]))
end

DBInterface.execute(conn, "Delete * FROM CurrentEnergyCost")

for row in eachrow(currentEnergyCost)
    DBInterface.execute(conn, "INSERT INTO CurrentEnergyCost ( FinishedGoodID, EnergyCost ) values( $(row.FinishedGoodID) , $(row.EnergyCost))")
end

DBInterface.close!(conn)