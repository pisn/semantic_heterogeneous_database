library(dplyr)
library(readr)
library(data.table)
library(ggplot2)
library(scales)

setwd('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos')

results <- list.files(path='/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos') %>% 
  lapply(read_csv) %>% 
  bind_rows 

## Calculating statistics over insertion phase data

insertion_phase_mean = aggregate(results$insertion_phase, list(results$records, results$insert_first), FUN=mean)
names(insertion_phase_mean) = c('number_of_records','insertion_type','mean')

insertion_phase_sd = aggregate(results$insertion_phase, list(results$records, results$insert_first), FUN=sd)
names(insertion_phase_sd) = c('number_of_records','insertion_type','sd')

insertion_phase_count = aggregate(results$insertion_phase, list(results$records, results$insert_first), FUN=length)
names(insertion_phase_count) = c('number_of_records','insertion_type','count')

insertion_phase = merge(insertion_phase_mean, insertion_phase_sd, by=c('number_of_records','insertion_type'))
insertion_phase = merge(insertion_phase, insertion_phase_count, by=c('number_of_records','insertion_type'))
insertion_phase$count = as.numeric(insertion_phase$count)

confidence_interval = function(x) {
  count = as.numeric(x['count'])
  dev = as.numeric(x['sd'])
  
  error = qt(0.95, df=count-1)*dev/sqrt(count)
  return (error)
}

errors = apply(insertion_phase, 1, confidence_interval)
insertion_phase$error = errors
insertion_phase$lower_bound = insertion_phase$mean - insertion_phase$error
insertion_phase$upper_bound = insertion_phase$mean + insertion_phase$error

##Plot 
insertion_first = insertion_phase[insertion_phase$insertion_type == 'insertion_first',]
operations_first = insertion_phase[insertion_phase$insertion_type == 'operations_first',]


ggplot() + 
  geom_line(data=insertion_first, aes(x = number_of_records, y = mean, colour='Insertions-First')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=insertion_first) +
  geom_point(data=insertion_first, aes(x = number_of_records, y = mean, colour='Insertions-First'), size=3) + 
  geom_line(data=operations_first, aes(x = number_of_records, y = mean, colour='Operations-First')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=operations_first) +
  geom_point(data=operations_first, aes(x = number_of_records, y = mean, colour='Operations-First'), size=3) + 
  xlab('Number of Documents') + 
  ylab('Execution Time (s)') +
  scale_x_continuous(labels = comma_format(big.mark = ".")) +
  scale_colour_manual('', breaks=c('Insertions-First','Operations-First'), values=c('red','blue')) + 
  theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
        legend.position = c(.05, .95),
        legend.justification = c("left", "top"),
        legend.box.just = "left",
        legend.margin = margin(6, 6, 6, 6),
        text=element_text(size=18))


### Calculating statistics over different scenarios and different number of operations execution_time for the same number of records (500k)
results_500 = results[results$records == 500000,]
scenario_mean = aggregate(results_500$operations_phase, list(results_500$number_of_operations, results_500$update_percent), FUN=mean)
names(scenario_mean) = c('number_of_operations','update_percent','mean')

scenario_sd = aggregate(results_500$operations_phase, list(results_500$number_of_operations, results_500$update_percent), FUN=sd)
names(scenario_sd) = c('number_of_operations','update_percent','sd')

scenario_count = aggregate(results_500$operations_phase, list(results_500$number_of_operations, results_500$update_percent), FUN=length)
names(scenario_count) = c('number_of_operations','update_percent','count')

scenario = merge(scenario_mean, scenario_sd, by=c('number_of_operations','update_percent'))
scenario = merge(scenario, scenario_count, by=c('number_of_operations','update_percent'))

errors = apply(scenario, 1, confidence_interval)
scenario$error = errors
scenario$lower_bound = scenario$mean - scenario$error
scenario$upper_bound = scenario$mean + scenario$error

##Plot 
read_only = scenario[scenario$update_percent == 0,]
write_only = scenario[scenario$update_percent == 1,]
read_heavy = scenario[scenario$update_percent == 0.05,]
write_heavy = scenario[scenario$update_percent == 0.95,]
mixed = scenario[scenario$update_percent == 0.5,]


ggplot() + 
  geom_line(data=read_only, aes(x = number_of_operations, y = mean, colour='Read-Only')) + 
  geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_only) +
  geom_point(data=read_only, aes(x = number_of_operations, y = mean, colour='Read-Only'), size=3) + 
  
  geom_line(data=write_only, aes(x = number_of_operations, y = mean, colour='Write-Only')) + 
  geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_only) +
  geom_point(data=write_only, aes(x = number_of_operations, y = mean, colour='Write-Only'), size=3) + 
  
  geom_line(data=read_heavy, aes(x = number_of_operations, y = mean, colour='Read-Heavy')) + 
  geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_heavy) +
  geom_point(data=read_heavy, aes(x = number_of_operations, y = mean, colour='Read-Heavy'), size=3) + 
  
  geom_line(data=write_heavy, aes(x = number_of_operations, y = mean, colour='Write-Heavy')) + 
  geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_heavy) +
  geom_point(data=write_heavy, aes(x = number_of_operations, y = mean, colour='Write-Heavy'), size=3) + 
  
  geom_line(data=mixed, aes(x = number_of_operations, y = mean, colour='50/50')) + 
  geom_ribbon(aes(x=number_of_operations, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=mixed) +
  geom_point(data=mixed, aes(x = number_of_operations, y = mean, colour='50/50'), size=3) + 
  
  xlab('Number of insert/select Operations') + 
  ylab('Execution Time (s)') +
  scale_colour_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), values=c('red','darksalmon','purple','cornflowerblue','blue')) + 
  scale_x_continuous(breaks=c(100,200,500,700,1000)) +
  theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
        legend.position = c(.05, .95),
        legend.justification = c("left", "top"),
        legend.box.just = "left",
        legend.margin = margin(6, 6, 6, 6),
        text=element_text(size=18))



### Calculating statistics over different scenarios and different number of records operation phase execution_time for the same number of operations (500)
results_500 = results[results$number_of_operations == 500,]
scenario_mean = aggregate(results_500$operations_phase, list(results_500$records, results_500$update_percent), FUN=mean)
names(scenario_mean) = c('number_of_records','update_percent','mean')

scenario_sd = aggregate(results_500$operations_phase, list(results_500$records, results_500$update_percent), FUN=sd)
names(scenario_sd) = c('number_of_records','update_percent','sd')

scenario_count = aggregate(results_500$operations_phase, list(results_500$records, results_500$update_percent), FUN=length)
names(scenario_count) = c('number_of_records','update_percent','count')

scenario = merge(scenario_mean, scenario_sd, by=c('number_of_records','update_percent'))
scenario = merge(scenario, scenario_count, by=c('number_of_records','update_percent'))

errors = apply(scenario, 1, confidence_interval)
scenario$error = errors
scenario$lower_bound = scenario$mean - scenario$error
scenario$upper_bound = scenario$mean + scenario$error

##Plot 
read_only = scenario[scenario$update_percent == 0,]
write_only = scenario[scenario$update_percent == 1,]
read_heavy = scenario[scenario$update_percent == 0.05,]
write_heavy = scenario[scenario$update_percent == 0.95,]
mixed = scenario[scenario$update_percent == 0.5,]


ggplot() + 
  geom_line(data=read_only, aes(x = number_of_records, y = mean, colour='Read-Only')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_only) +
  geom_point(data=read_only, aes(x = number_of_records, y = mean, colour='Read-Only'), size=3) + 
  
  geom_line(data=write_only, aes(x = number_of_records, y = mean, colour='Write-Only')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_only) +
  geom_point(data=write_only, aes(x = number_of_records, y = mean, colour='Write-Only'), size=3) + 
  
  geom_line(data=read_heavy, aes(x = number_of_records, y = mean, colour='Read-Heavy')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_heavy) +
  geom_point(data=read_heavy, aes(x = number_of_records, y = mean, colour='Read-Heavy'), size=3) + 
  
  geom_line(data=write_heavy, aes(x = number_of_records, y = mean, colour='Write-Heavy')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_heavy) +
  geom_point(data=write_heavy, aes(x = number_of_records, y = mean, colour='Write-Heavy'), size=3) + 
  
  geom_line(data=mixed, aes(x = number_of_records, y = mean, colour='50/50')) + 
  geom_ribbon(aes(x=number_of_records, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=mixed) +
  geom_point(data=mixed, aes(x = number_of_records, y = mean, colour='50/50'), size=3) + 
  
  xlab('Number of Documents') + 
  ylab('Execution Time (s)') +
  ylim(0,6) +
  scale_colour_manual('', breaks=c('Read-Only','Read-Heavy','50/50','Write-Heavy','Write-Only'), values=c('blue','cornflowerblue','purple','darksalmon','red')) + 
  scale_x_continuous(breaks=c(100000,200000,300000,400000,500000),labels = comma_format(big.mark = ".")) +
  theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
        legend.position = c(0, .95),
        legend.justification = c("left", "top"),
        legend.box.just = "left",
        legend.margin = margin(6, 6, 6, 6),
        legend.direction = 'horizontal',
        text=element_text(size=17))


###### Comparison over semantic evolution operations

setwd('~/Documents/USP/Mestrado/Pesquisa/mongo-test/analysis/results_2')

results <- list.files(path='~/Documents/USP/Mestrado/Pesquisa/mongo-test/analysis/results_2') %>% 
  lapply(read_csv) %>% 
  bind_rows 

results_500 = results[results$number_of_operations == 500,]

scenario_mean = aggregate(results_500$operations_phase, list(results_500$number_of_versions,results_500$update_percent), FUN=mean)
names(scenario_mean) = c('number_of_versions','update_percent','mean')

scenario_sd = aggregate(results_500$operations_phase, list(results_500$number_of_versions,results_500$update_percent), FUN=sd)
names(scenario_sd) = c('number_of_versions','update_percent','sd')

scenario_count = aggregate(results_500$operations_phase, list(results_500$number_of_versions,results_500$update_percent), FUN=length)
names(scenario_count) = c('number_of_versions','update_percent','count')

scenario = merge(scenario_mean, scenario_sd, by=c('number_of_versions','update_percent'))
scenario = merge(scenario, scenario_count, by=c('number_of_versions','update_percent'))

errors = apply(scenario, 1, confidence_interval)
scenario$error = errors
scenario$lower_bound = scenario$mean - scenario$error
scenario$upper_bound = scenario$mean + scenario$error

##Plot 
read_only = scenario[scenario$update_percent == 0,]
write_only = scenario[scenario$update_percent == 1,]
read_heavy = scenario[scenario$update_percent == 0.05,]
write_heavy = scenario[scenario$update_percent == 0.95,]
mixed = scenario[scenario$update_percent == 0.5,]


ggplot() + 
  geom_line(data=read_only, aes(x = number_of_versions, y = mean, colour='Read-Only')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_only) +
  geom_point(data=read_only, aes(x = number_of_versions, y = mean, colour='Read-Only'), size=3) + 
  
  geom_line(data=write_only, aes(x = number_of_versions, y = mean, colour='Write-Only')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_only) +
  geom_point(data=write_only, aes(x = number_of_versions, y = mean, colour='Write-Only'), size=3) + 
  
  geom_line(data=read_heavy, aes(x = number_of_versions, y = mean, colour='Read-Heavy')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_heavy) +
  geom_point(data=read_heavy, aes(x = number_of_versions, y = mean, colour='Read-Heavy'), size=3) + 
  
  geom_line(data=write_heavy, aes(x = number_of_versions, y = mean, colour='Write-Heavy')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_heavy) +
  geom_point(data=write_heavy, aes(x = number_of_versions, y = mean, colour='Write-Heavy'), size=3) + 
  
  geom_line(data=mixed, aes(x = number_of_versions, y = mean, colour='50/50')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=mixed) +
  geom_point(data=mixed, aes(x = number_of_versions, y = mean, colour='50/50'), size=3) + 
  
  xlab('Number of semantic evolution operations') + 
  ylab('Execution Time (s)') +
  ylim(0,6) + 
  scale_colour_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), values=c('red','darksalmon','purple','cornflowerblue','blue')) + 
  scale_x_continuous(breaks=c(5,10,15)) +
  theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
        legend.position = c(0, .95),
        legend.justification = c("left", "top"),
        legend.box.just = "left",
        legend.margin = margin(6, 6, 6, 6),
        legend.direction = 'horizontal',
        text=element_text(size=17))


insertion_phase_mean = aggregate(results_500$insertion_phase, list(results_500$number_of_versions, results_500$insert_first), FUN=mean)
names(insertion_phase_mean) = c('number_of_versions','insertion_type','mean')

insertion_phase_sd = aggregate(results_500$insertion_phase, list(results_500$number_of_versions, results_500$insert_first), FUN=sd)
names(insertion_phase_sd) = c('number_of_versions','insertion_type','sd')

insertion_phase_count = aggregate(results_500$insertion_phase, list(results_500$number_of_versions, results_500$insert_first), FUN=length)
names(insertion_phase_count) = c('number_of_versions','insertion_type','count')

insertion_phase = merge(insertion_phase_mean, insertion_phase_sd, by=c('number_of_versions','insertion_type'))
insertion_phase = merge(insertion_phase, insertion_phase_count, by=c('number_of_versions','insertion_type'))
insertion_phase$count = as.numeric(insertion_phase$count)


errors = apply(insertion_phase, 1, confidence_interval)
insertion_phase$error = errors
insertion_phase$lower_bound = insertion_phase$mean - insertion_phase$error
insertion_phase$upper_bound = insertion_phase$mean + insertion_phase$error

##Plot 
insertion_first = insertion_phase[insertion_phase$insertion_type == 'insertion_first',]
operations_first = insertion_phase[insertion_phase$insertion_type == 'operations_first',]


ggplot() + 
  geom_line(data=insertion_first, aes(x = number_of_versions, y = mean, colour='Insertions-First')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=insertion_first) +
  geom_point(data=insertion_first, aes(x = number_of_versions, y = mean, colour='Insertions-First'), size=3) + 
  geom_line(data=operations_first, aes(x = number_of_versions, y = mean, colour='Operations-First')) + 
  geom_ribbon(aes(x=number_of_versions, y=mean, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=operations_first) +
  geom_point(data=operations_first, aes(x = number_of_versions, y = mean, colour='Operations-First'), size=3) + 
  xlab('Number of semantic evolution operations') + 
  ylab('Execution Time (s)') +
  scale_x_continuous(breaks=c(5,10,15)) +
  scale_colour_manual('', breaks=c('Insertions-First','Operations-First'), values=c('red','blue')) + 
  theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
        legend.position = c(.05, .95),
        legend.justification = c("left", "top"),
        legend.box.just = "left",
        legend.margin = margin(6, 6, 6, 6),
        text=element_text(size=18))


### Table

confidence_interval = function(x) {
  count = as.numeric(x['count'])
  dev = as.numeric(x['sd'])
  
  error = qt(0.95, df=count-1)*dev/sqrt(count)
  return (error)
}

read_only
baseline = results[results$update_percent == 0,'operations_baseline']
avg = mean(baseline$operations_baseline)
dev = sd(baseline$operations_baseline)
n = length(baseline$operations_baseline)
error = qt(0.95, df=n-1)*dev/sqrt(n)


write_only
summary(results[results$update_percent == 1,'operations_baseline'])

baseline = results[results$update_percent == 1,'operations_baseline']
avg = mean(baseline$operations_baseline)
dev = sd(baseline$operations_baseline)
n = length(baseline$operations_baseline)
error = qt(0.95, df=n-1)*dev/sqrt(n)
error
