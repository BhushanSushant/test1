# Databricks notebook source
generate_user_version_folders_elast_opt <- function(user,version,bu,base_directory){
  invisible(
    dir.create(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/")),recursive = T, showWarnings = F )
  )
  
#   invisible(
#     file.remove(list.files(file.path(paste0("/dbfs/SE_Repo/Elasticity_Modelling/Log/Elasticity_opti_log/",user,"/v",version,"/")),full.names = T))
#   )
  }

# COMMAND ----------

compCost<-function(X, y, theta){
  m <- length(y)
  J <- sum((X%*%theta- y)^2)/(2*m)
  return(J)
}

# COMMAND ----------

gradDescent<-function(X, y, theta, alpha, num_iters){
  m <- length(y)
  J_hist <- rep(0, num_iters)
  for(i in 1:num_iters){
    
    # this is a vectorized form for the gradient of the cost function
    # X is a 100x5 matrix, theta is a 5x1 column vector, y is a 100x1 column vector
    # X transpose is a 5x100 matrix. So t(X)%*%(X%*%theta - y) is a 5x1 column vector
    theta <- theta - alpha*(1/m)*(t(X)%*%(X%*%theta - y))
    
    # this for-loop records the cost history for every iterative move of the gradient descent,
    # and it is obtained for plotting number of iterations against cost history.
    J_hist[i]  <- compCost(X, y, theta)
    #constraint
  }
  # for a R function to return two values, we need to use a list to store them:
  # if(theta[2] > -0.5 | theta[2] < -2){
  # 
  #   maxdiff <- (-0.5 - theta[2] )
  #   mindiff <- (-2 - theta[2] )
  #   if(maxdiff>mindiff){
  #     theta[2] <- -0.5
  #   }else{
  #     theta[2] <- -2
  #   }
  # }else{
  #   theta <- theta
  # }
  results<-list(theta, J_hist)
  return(results)
}
