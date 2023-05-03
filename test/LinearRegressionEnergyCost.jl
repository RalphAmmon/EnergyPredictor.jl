using CSV, DataFrames, GLM

trainingData = CSV.read("data/flat/RegressionExample.csv", DataFrame)

groups = groupby(trainingData, :FinishedGoodID)

modelsDF = DataFrame(FinishedGoodID=Vector{Int64}(),
    LinModel=Vector{}())

for group in groups
    println(group.FinishedGoodID[1])
    push!(modelsDF, (FinishedGoodID=group.FinishedGoodID[1], LinModel=lm(@formula(EnergyCost ~ ElectricityPriceBE + NaturalGasPriceBE + ElectricityPriceFR + NaturalGasPriceFR), group)))
end

newPrices = DataFrame(
    ElectricityPriceBE=[505.3],
    NaturalGasPriceBE=[240.85],
    ElectricityPriceFR=[643.7],
    NaturalGasPriceFR=[240.85]
)

newEnergyCost = DataFrame(FinishedGoodID=Vector{Int64}(),
    EnergyCost=Vector{Float64}())

for row in eachrow(modelsDF)
     energyCost = predict(row.LinModel, newPrices)
    push!(newEnergyCost, (FinishedGoodID = row.FinishedGoodID, EnergyCost = energyCost[1]))
end

CSV.write("data/flat/NewEnergyCost.csv", newEnergyCost)