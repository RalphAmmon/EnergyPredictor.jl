using MultivariateStats, CSV, DataFrames

# load Tariffs
tariffs = DataFrame(CSV.File("./data/flat/Tariffs.csv"))

# traning set tariffs
xTariffs = convert(Array, Matrix(tariffs[:, 2:end]))

# train a PCA tariff model, allowing up to 3 dimensions
pcaModel = fit(PCA, xTariffs; maxoutdim=3)

uLoad = loadings(pcaModel)

tariffsPCA = DataFrame(PC1 = -1.0 * uLoad[:,1],PC2 = -1.0 * uLoad[:,2],PC3 = uLoad[:,3])