library(dplyr)
library(readr)
library(data.table)
library(ggplot2)
library(scales)

setwd('/home/pedro/Documents/USP/Mestrado/Pesquisa/mongo-test/analysis/experiments_test1/')

results <- list.files(path='/home/pedro/Documents/USP/Mestrado/Pesquisa/mongo-test/analysis/experiments_test1/') %>% 
  lapply(read_csv, show_col_types=FALSE) %>% 
  bind_rows 

confidence_interval = function(x) {
  count = as.numeric(x['count'])
  dev = as.numeric(x['sd'])
  
  error = qt(0.95, df=count-1)*dev/sqrt(count)
  return (error)
}

### Lets start by comparing, for read-only scenarios, 200k records, preprocess vs rewrite, 10 versions
#results_readonly = results[(results$update_percent == 0),]
results_readheavy = results[(results$update_percent == 0.05),]
results_50 = results[(results$update_percent == 0.5),]
results_writeheavy = results[(results$update_percent == 0.95),]
#results_writeonly = results[(results$update_percent == 1),]

generate_scenario = function (scenario, scenario_title) {
  scenario_mean = aggregate(scenario$operations_phase, list(scenario$number_of_operations, scenario$operation_mode), FUN=mean)
  names(scenario_mean) = c('number_of_operations','operation_mode','mean')
  
  scenario_sd = aggregate(scenario$operations_phase, list(scenario$number_of_operations, scenario$operation_mode), FUN=sd)
  names(scenario_sd) = c('number_of_operations','operation_mode','sd')
  
  scenario_count = aggregate(scenario$operations_phase, list(scenario$number_of_operations, scenario$operation_mode), FUN=length)
  names(scenario_count) = c('number_of_operations','operation_mode','count')
  
  scenario = merge(scenario_mean, scenario_sd, by=c('number_of_operations','operation_mode'))
  scenario = merge(scenario, scenario_count, by=c('number_of_operations','operation_mode'))
  
  errors = apply(scenario, 1, confidence_interval)
  scenario$error = errors
  scenario$lower_bound = scenario$mean - scenario$error
  scenario$upper_bound = scenario$mean + scenario$error
  
  ##Plot 
  preprocess = scenario[scenario$operation_mode == 'preprocess',]
  rewrite = scenario[scenario$operation_mode == 'rewrite',]
  
  ggplot() + 
    geom_line(data=preprocess, aes(x = number_of_operations, y = mean, colour='preprocess')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=preprocess) +
    geom_point(data=preprocess, aes(x = number_of_operations, y = mean, colour='preprocess'), size=3) + 
    
    geom_line(data=rewrite, aes(x = number_of_operations, y = mean, colour='rewrite')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=rewrite) +
    geom_point(data=rewrite, aes(x = number_of_operations, y = mean, colour='rewrite'), size=3) + 
    
    lab(title=scenario_title) +
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    scale_colour_manual('', breaks=c('preprocess','rewrite'), values=c('red','blue')) + 
    scale_x_continuous(breaks=seq(100,1000,100)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
  
}

#generate_scenario(results_readonly, 'Read-Only')
generate_scenario(results_readheavy, 'Read-Heavy')
generate_scenario(results_50, '50/50')
generate_scenario(results_writeheavy, 'Write-Heavy')
#generate_scenario(results_writeonly, 'Write-Only')

#results_readonly = results[(results$update_percent == 0),]
results_readheavy = results[(results$update_percent == 0.05),]
results_50 = results[(results$update_percent == 0.5),]
results_writeheavy = results[(results$update_percent == 0.95),]
#results_writeonly = results[(results$update_percent == 1),]

