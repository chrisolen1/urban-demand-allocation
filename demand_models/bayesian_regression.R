
library(rstan)

dataPath <- "/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation/demand_models"
dat <- read.csv("../../data/demand_model.csv")
dat <- subset(dat, select = -c(year, latitude, longitude))

x <- subset(dat, select = -c(sales_volume_location))
y <- dat$sales_volume_location
Ntotal <- dim(x)[1]
Nx <- dim(x)[2]
dataListRegression <- list(Ntotal=Ntotal,
                           y=y,
                           x=x,
                           Nx=Nx)

modelString <- "
data { 
    int<lower=1> Ntotal; 
    int<lower=1> Nx; 
    vector[Ntotal] y; 
    matrix[Ntotal, Nx] x; 
}
transformed data { 
    real meanY;
    real sdY;
    vector[Ntotal] zy; 
    vector[Nx] meanX;
    vector[Nx] sdX;
    matrix[Ntotal, Nx] zx; 
    meanY = mean(y);
    sdY = sd(y);
    zy = (y - meanY) / sdY;
    for ( j in 1:Nx ) {
        meanX[j] = mean(x[,j]);
        sdX[j] = sd(x[,j]);
        for ( i in 1:Ntotal ) {
            zx[i,j] = ( x[i,j] - meanX[j] ) / sdX[j];
        }
    }
}
parameters {
    real zbeta0;
    vector[Nx] zbeta;
    real<lower=0> sigmaBeta0;
    vector<lower=0>[Nx] sigmaBeta;
    real<lower=0> zsigma;
    real<lower=0> tau;
    real<lower=0> nu;
}
model {
    sigmaBeta0 ~ gamma(3.0, 2.0); // mode=(kappa-1)/theta, var=kappa/theta^2
    zbeta0 ~ student_t(1.0/30.0,0,sigmaBeta0); 
    sigmaBeta ~ gamma(3.0, 2.0); // mode=(kappa-1)/theta, var=kappa/theta^2
    zbeta ~ student_t(1.0/30.0,0,sigmaBeta);
    zsigma ~ gamma(3,1);
    nu ~ gamma(3.0, 2.0);
    tau ~ gamma(nu/2.0,nu/2.0);
    zy ~ normal(zbeta0 + zx * zbeta, 1/sqrt(tau)); 
}
generated quantities { 
    real beta0; 
    vector[Nx] beta;
    beta = sdY * ( zbeta ./ sdX );
    beta0 = zbeta0 * sdY  + meanY - sdY * sum( zbeta .* meanX ./ sdX );
} "

RobustMultipleRegressionDso <- stan_model( model_code=modelString )

fit  <- sampling(RobustMultipleRegressionDso,
                 data=dataListRegression,
                 pars=c('beta0', 'beta','zbeta0','zbeta','sigmaBeta0','sigmaBeta','zsigma', 'nu', 'tau'),
                 iter=25000, chains = 8, cores = 4,
                 control = list(adapt_delta = .90, max_treedepth = 20))

demand_features <- colnames(subset(dat, select = -c(sales_volume_location)))
demand_coefs <- c(median(as.matrix(fit)[,2]),
                  median(as.matrix(fit)[,3]),
                  median(as.matrix(fit)[,4]),
                  median(as.matrix(fit)[,5]))
write.table(demand_coefs, "../../data/betas.csv", row.names = FALSE, col.names = FALSE)


