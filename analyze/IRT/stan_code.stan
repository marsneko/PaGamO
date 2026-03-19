data {
  int<lower=0> N;
  int<lower=1> Userid_Num;
  int<lower=1> Sec_Num;

  array[N] int<lower=1, upper=Userid_Num> A;
  array[N] int<lower=1, upper=Sec_Num> B;
  array[N] int<lower=0, upper=1> D;
}

parameters {
  vector[Userid_Num] alpha;
  vector[Sec_Num] beta;
}

model {
  // priors
  alpha ~ normal(0, 100);
  beta  ~ normal(0, 100);

  // likelihood
  for (i in 1:N) {
    D[i] ~ bernoulli_logit(alpha[A[i]] - beta[B[i]]);
  }
}