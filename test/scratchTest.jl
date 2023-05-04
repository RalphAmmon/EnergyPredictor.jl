using DataFrames


conn = ODBC.Connection(dsin)
trainingData = DBInterface.execute(conn, "SELECT * FROM $(modelConfigRow.TrainingDS)") |> DataFrame
DBInterface.close!(conn)

myLM = modelsDF.LinModel[1]
coeftable(myLM)
formula(myLM)


model_X_FieldsID = vcat([1], xFields.Model_X_FieldsID)
lmDF = DataFrame(ModelID=model)

(
    ModelID=modelVector,
    Model_X_FieldID=model_X_FieldID,
    Coef=coefTbl.cols[1],
    StdError=coefTbl.cols[2],
    t=coefTbl.cols[3],
    PrGtAbsT=coefTbl.cols[4],
    Lower95Percent=coefTbl.cols[5],
    Upper95Percent=coefTbl.cols[6]
)

mcDF = DataFrame(
    ModelID=Vector{Int64}()
)

vcat(mcDF,
    DataFrame(
        "ModelID" => modelVector
    )
)
DataFrame(
    ModelID=modelVector
)

doLoop = true
for i in 1:2
    doLoop = false
end
pwd()
if last(pwd(), 3) == "src"
    cd("..")
end
pwd()


tData = DBInterface.execute(conn,
    "SELECT * FROM ProductionSP"
) |> DataFrame