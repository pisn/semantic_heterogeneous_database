library(dplyr)
library(readr)
library(data.table)
library(ggplot2)
library(scales)

setwd('/home/pedro/Documents/USP/Mestrado/Pesquisa/mongo-test/analysis/experiments_test1/')

results <- list.files(path='/home/pedro/Documents/USP/Mestrado/Pesquisa/mongo-test/analysis/experiments_test1/', pattern='.*csv') %>% 
  lapply(read_csv, show_col_types=FALSE) %>% 
  bind_rows 

confidence_interval = function(x) {
  count = as.numeric(x['count'])
  dev = as.numeric(x['sd'])
  
  error = qt(0.95, df=count-1)*dev/sqrt(count)
  return (error)
}

### Lets start by comparing, for read-only scenarios, 200k records, preprocess vs rewrite, 10 versions
results_preprocess = results[(results$operation_mode == 'preprocess'),]
results_rewrite = results[(results$operation_mode == 'rewrite'),]


generate_scenario = function (scenario, scenario_title) {
  scenario_mean = aggregate(scenario$operations_phase, list(scenario$number_of_operations, scenario$update_percent), FUN=mean)
  names(scenario_mean) = c('number_of_operations','update_percent','mean')
  
  scenario_sd = aggregate(scenario$operations_phase, list(scenario$number_of_operations, scenario$update_percent), FUN=sd)
  names(scenario_sd) = c('number_of_operations','update_percent','sd')
  
  scenario_count = aggregate(scenario$operations_phase, list(scenario$number_of_operations, scenario$update_percent), FUN=length)
  names(scenario_count) = c('number_of_operations','update_percent','count')
  
  scenario = merge(scenario_mean, scenario_sd, by=c('number_of_operations','update_percent'))
  scenario = merge(scenario, scenario_count, by=c('number_of_operations','update_percent'))
  
  errors = apply(scenario, 1, confidence_interval)
  scenario$error = errors
  scenario$lower_bound = scenario$mean - scenario$error
  scenario$upper_bound = scenario$mean + scenario$error
  
  ##Plot 
  results_readonly = scenario[(scenario$update_percent == 0),]
  results_readheavy = scenario[(scenario$update_percent == 0.05),]
  results_50 = scenario[(scenario$update_percent == 0.5),]
  results_writeheavy = scenario[(scenario$update_percent == 0.95),]
  results_writeonly = scenario[(scenario$update_percent == 1),]
  
  ggplot() + 
    geom_line(data=results_readonly, aes(x = number_of_operations, y = mean, colour='read_only')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=results_readonly) +
    geom_point(data=results_readonly, aes(x = number_of_operations, y = mean, colour='read_only'), size=3) + 
    
    geom_line(data=results_readheavy, aes(x = number_of_operations, y = mean, colour='read_heavy')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=results_readheavy) +
    geom_point(data=results_readheavy, aes(x = number_of_operations, y = mean, colour='read_heavy'), size=3) + 
    
    geom_line(data=results_50, aes(x = number_of_operations, y = mean, colour='50/50')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=results_50) +
    geom_point(data=results_50, aes(x = number_of_operations, y = mean, colour='50/50'), size=3) + 
    
    geom_line(data=results_writeheavy, aes(x = number_of_operations, y = mean, colour='write_heavy')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=results_writeheavy) +
    geom_point(data=results_writeheavy, aes(x = number_of_operations, y = mean, colour='write_heavy'), size=3) + 
    
    geom_line(data=results_writeonly, aes(x = number_of_operations, y = mean, colour='write_only')) + 
    geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=results_writeonly) +
    geom_point(data=results_writeonly, aes(x = number_of_operations, y = mean, colour='write_only'), size=3) + 
    
    ggtitle(scenario_title) +
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    scale_colour_manual('', breaks=c('read_only','read_heavy','50/50','write_heavy','write_only'), values=c('red','orange','violet','purple','blue')) + 
    scale_x_continuous(breaks=seq(100,1000,100)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
  
}

generate_scenario(results_preprocess, 'Pre-Process')
generate_scenario(results_rewrite, 'Rewrite')



