using DataFrames

df = DataFrame(a = [1, 2], b = ["x", "y"])

for row in eachrow(df)
    t = typeof(row)
    println(t)
end