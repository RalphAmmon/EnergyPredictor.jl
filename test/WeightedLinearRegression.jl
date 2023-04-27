using GLM, DataFrames

# Generate some example data
x = [1, 2, 3, 4, 5]
y = [1.1, 2.0, 2.9, 3.8, 4.7]
w = [1, 1, 2, 1, 1]

# Fit a weighted linear regression model
model = glm(@formula(y ~ x), DataFrame(x=x,y=y), Normal(), IdentityLink(),wts=w)

# Print the model summary
println(summary(model))

# Get the estimated coefficients and standard errors
coeftable(model)

# Make predictions using the model
x_new = [6, 7, 8]
y_new = predict(model, DataFrame(x=x_new))

# Print the predicted values
println(y_new)