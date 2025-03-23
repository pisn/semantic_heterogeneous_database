library(dplyr)
library(readr)
library(data.table)
library(ggplot2)
library(scales)

setwd('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/images/')

process_results <- function(folder) {
  results <- list.files(path=folder, pattern="^result.*\\.txt$", full.names=TRUE) %>%
  lapply(function(file) {
    lines <- readLines(file)
    test_start_line <- lines[grep("^Test Start", lines)]
    last_line <- strsplit(lines[length(lines)], ";")[[1]]
    c(strsplit(test_start_line, ";")[[1]], last_line)
  }) %>%
  do.call(rbind, .) %>%
  as.data.frame(stringsAsFactors = FALSE)


colnames(results) <- c("test_start", "method", "insertion_method","number_of_operations", "scenario", "heterogeneity_level", "trial", "t_header", "time_taken")

results_grouped <- results %>%
  group_by(method, number_of_operations, scenario, heterogeneity_level) %>%
  summarise(
    mean_time_taken = mean(as.numeric(time_taken)),
    sd_time_taken = sd(as.numeric(time_taken)),
    count = n()
  ) %>%
  mutate(
    error = qt(0.975, df = count - 1) * sd_time_taken / sqrt(count),
    lower_bound = mean_time_taken - error,
    upper_bound = mean_time_taken + error
  )

  return (results_grouped)
}


plot_execution_time_scenarios <- function(results_grouped ,heterogeneity_level, title) {
  read_only <- results_grouped[results_grouped$scenario == 0 & results_grouped$heterogeneity_level == heterogeneity_level,]
  write_only <- results_grouped[results_grouped$scenario == 1 & results_grouped$heterogeneity_level == heterogeneity_level,]
  read_heavy <- results_grouped[results_grouped$scenario == 0.05 & results_grouped$heterogeneity_level == heterogeneity_level,]
  write_heavy <- results_grouped[results_grouped$scenario == 0.95 & results_grouped$heterogeneity_level == heterogeneity_level,]
  mixed <- results_grouped[results_grouped$scenario == 0.5 & results_grouped$heterogeneity_level == heterogeneity_level,]
  
  ggplot() + 
    geom_line(data=read_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Read-Only')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_only) +
    geom_point(data=read_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Read-Only'), size=3) + 
    
    geom_line(data=write_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Write-Only')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_only) +
    geom_point(data=write_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Write-Only'), size=3) + 
    
    geom_line(data=read_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Read-Heavy')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=read_heavy) +
    geom_point(data=read_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Read-Heavy'), size=3) + 
    
    geom_line(data=write_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Write-Heavy')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=write_heavy) +
    geom_point(data=write_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Write-Heavy'), size=3) + 
    
    geom_line(data=mixed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='50/50')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=mixed) +
    geom_point(data=mixed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='50/50'), size=3) + 
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), values=c('red','darksalmon','purple','cornflowerblue','blue')) + 
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size=23),
          text=element_text(size=25))
}


plot_execution_time_fields <- function(results_grouped_onefield, results_grouped_twofields, heterogeneity_level, scenario, title) {
  onefield <- results_grouped_onefield[results_grouped_onefield$scenario == scenario & results_grouped_onefield$heterogeneity_level == heterogeneity_level,]
  twofields <- results_grouped_twofields[results_grouped_twofields$scenario == scenario & results_grouped_twofields$heterogeneity_level == heterogeneity_level,]
  
  ggplot() + 
    geom_line(data=onefield, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='One Het.Field')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=onefield) +
    geom_point(data=onefield, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='One Het.Field'), size=3) + 
    
    geom_line(data=twofields, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Two Het. Fields')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=twofields) +
    geom_point(data=twofields, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Two Het. Fields'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c("One Het.Field","Two Het. Fields"), values=c('red','blue')) + 
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
}


plot_execution_time_heterogeneitylevel <- function(results_grouped, scenario, title, scenario_name) {
  het30 <- results_grouped[results_grouped$scenario == scenario & results_grouped$heterogeneity_level == 0.3,]
  het15 <- results_grouped[results_grouped$scenario == scenario & results_grouped$heterogeneity_level == 0.15,]
  
  ggplot() + 
    geom_line(data=het15, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour=paste0(scenario_name," 15%"))) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=het15) +
    geom_point(data=het15, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour=paste0(scenario_name," 15%")), size=3) + 
    
    geom_line(data=het30, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour=paste0(scenario_name," 30%"))) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=het30) +
    geom_point(data=het30, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour=paste0(scenario_name," 30%")), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c(paste0(scenario_name," 15%"),paste0(scenario_name," 30%")), values=c('red','blue')) + 
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
}


plot_execution_time_heterogeneitylevel_bars <- function(results_grouped, number_of_operations, title) {
  het30 <- results_grouped[results_grouped$heterogeneity_level == 0.3,]
  het15 <- results_grouped[results_grouped$heterogeneity_level == 0.15,]
  
  het30$heterogeneity_level = '30%'
  het15$heterogeneity_level = '15%'

  results = rbind(het30,het15)
  results = results[results$number_of_operations==number_of_operations,]
  
  results$scenario = factor(results$scenario, levels = c(0,0.05,0.5,0.95,1), labels=c("Read-Only","Read-Heavy","50/50","Write-Heavy","Write-Only"))
  
  ggplot() + 
    geom_bar(data=results, aes(x = scenario, y = mean_time_taken, fill=heterogeneity_level), stat="identity", position=position_dodge(width=0.8), width = 0.7) +     
    geom_errorbar(data=results, aes(x = scenario, ymin = lower_bound, ymax = upper_bound, group = heterogeneity_level), 
                  width = 0.2, position=position_dodge(width=0.8), size=1) +
    
    xlab('Scenario') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_fill_manual('', breaks=c('30%','15%'), 
                      values=c('blue','red')) + 
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = "bottom",
          legend.justification = "center",
          # legend.box.just = "center",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size=25),
          legend.title = element_text(size=20),
          axis.text.x= element_text(angle=45, hjust=1, size=20),
          text=element_text(size=25))
}


plot_execution_time_opsmethod <- function(results_grouped_preprocess, results_grouped_rewrite, scenario, heterogeneity_level, title) {
  preprocess <- results_grouped_preprocess[results_grouped_preprocess$scenario == scenario & results_grouped_preprocess$heterogeneity_level == heterogeneity_level,]
  rewrite <- results_grouped_rewrite[results_grouped_rewrite$scenario == scenario & results_grouped_rewrite$heterogeneity_level == heterogeneity_level,]
  
  
  ggplot() + 
    geom_line(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Preprocess')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=preprocess) +
    geom_point(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Preprocess'), size=3) + 
    
    geom_line(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Rewrite')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=rewrite) +
    geom_point(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Rewrite'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c('Preprocess','Rewrite'), values=c('red','blue')) + 
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          legend.text= element_text(size=23),
          text=element_text(size=25))
}


plot_execution_time_hetlevel <- function(results_grouped_preprocess, results_grouped_rewrite, scenario, number_of_operations, title) {
  preprocess <- results_grouped_preprocess[results_grouped_preprocess$scenario == scenario & results_grouped_preprocess$number_of_operations == number_of_operations,]
  rewrite <- results_grouped_rewrite[results_grouped_rewrite$scenario == scenario & results_grouped_rewrite$number_of_operations == number_of_operations,]
  
  
  ggplot() + 
    geom_line(data=preprocess, aes(x = as.numeric(heterogeneity_level), y = mean_time_taken, colour='Preprocess')) + 
    geom_ribbon(aes(x=as.numeric(heterogeneity_level), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=preprocess) +
    geom_point(data=preprocess, aes(x = as.numeric(heterogeneity_level), y = mean_time_taken, colour='Preprocess'), size=3) + 
    
    geom_line(data=rewrite, aes(x = as.numeric(heterogeneity_level), y = mean_time_taken, colour='Rewrite')) + 
    geom_ribbon(aes(x=as.numeric(heterogeneity_level), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=rewrite) +
    geom_point(data=rewrite, aes(x = as.numeric(heterogeneity_level), y = mean_time_taken, colour='Rewrite'), size=3) +    
    
    
    xlab('Heterogeneity Level') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c('Preprocess','Rewrite'), values=c('red','blue')) + 
    scale_x_continuous(breaks=c(0.15,0.3), labels=c('15%','30%')) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .5),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
}

plot_execution_time_methods <- function(results_grouped_preprocess, results_grouped_rewrite, number_of_operations, heterogeneity_level, title, side_by_side = FALSE) {
  preprocess <- results_grouped_preprocess[results_grouped_preprocess$number_of_operations == number_of_operations & results_grouped_preprocess$heterogeneity_level == heterogeneity_level,]
  rewrite <- results_grouped_rewrite[results_grouped_rewrite$number_of_operations == number_of_operations & results_grouped_rewrite$heterogeneity_level == heterogeneity_level,]

  preprocess$scenario <- as.character(preprocess$scenario)
  rewrite$scenario <- as.character(rewrite$scenario)
  
  if (side_by_side) {
    p1 <- ggplot() + 
      geom_line(data=preprocess, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Preprocess')) + 
      geom_ribbon(aes(x=as.numeric(scenario), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=preprocess) +
      geom_point(data=preprocess, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Preprocess'), size=3) + 
      xlab('Scenario') + 
      ylab('Execution Time (s)') +
      scale_colour_manual('', breaks=c('Preprocess'), values=c('red')) +     
      scale_x_continuous(breaks=c(0,0.05,0.5,0.95,1), labels=c('Read-Only','Read-Heavy','50/50','Write-Heavy','Write-Only')) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
            legend.position = c(.05, .95),
            legend.justification = c("left", "top"),
            legend.box.just = "left",
            legend.margin = margin(6, 6, 6, 6),
            text=element_text(size=18))
    
    p2 <- ggplot() + 
      geom_line(data=rewrite, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Rewrite')) + 
      geom_ribbon(aes(x=as.numeric(scenario), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=rewrite) +
      geom_point(data=rewrite, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Rewrite'), size=3) + 
      xlab('Scenario') + 
      ylab('Execution Time (s)') +
      scale_colour_manual('', breaks=c('Rewrite'), values=c('blue')) +     
      scale_x_continuous(breaks=c(0,0.05,0.5,0.95,1), labels=c('Read-Only','Read-Heavy','50/50','Write-Heavy','Write-Only')) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
            legend.position = c(.65, .95),
            legend.justification = c("left", "top"),
            legend.box.just = "left",
            legend.margin = margin(6, 6, 6, 6),
            text=element_text(size=18))
    
    gridExtra::grid.arrange(p1, p2, ncol=2)
  } else {
    ggplot() + 
      geom_line(data=preprocess, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Preprocess')) + 
      geom_ribbon(aes(x=as.numeric(scenario), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=preprocess) +
      geom_point(data=preprocess, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Preprocess'), size=3) + 
      geom_line(data=rewrite, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Rewrite')) + 
      geom_ribbon(aes(x=as.numeric(scenario), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=rewrite) +
      geom_point(data=rewrite, aes(x = as.numeric(scenario), y = mean_time_taken, colour='Rewrite'), size=3) +    
      xlab('Scenario') + 
      ylab('Execution Time (s)') +
      ggtitle(title) +
      scale_colour_manual('', breaks=c('Preprocess','Rewrite'), values=c('red','blue')) +     
      scale_x_continuous(breaks=c(0,0.05,0.5,0.95,1), labels=c('Read-Only','Read-Heavy','50/50','Write-Heavy','Write-Only')) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
            legend.position = c(.65, .95),
            legend.justification = c("left", "top"),
            legend.box.just = "left",
            legend.margin = margin(6, 6, 6, 6),
            text=element_text(size=18))
  }
}

plot_execution_time_scenarios_bar <- function(results_preprocess, results_rewrite, heterogeneity_level, number_of_operations, title) {
  results_preprocess$approach = 'Preprocess'
  results_rewrite$approach = 'Rewrite'
  
  results = rbind(results_preprocess,results_rewrite)
  results = results[results$number_of_operations==number_of_operations,]
  results$scenario = factor(results$scenario, levels = c(0,0.05,0.5,0.95,1), labels=c("Read-Only","Read-Heavy","50/50","Write-Heavy","Write-Only"))
  
  ggplot() + 
    geom_bar(data=results, aes(x = scenario, y = mean_time_taken, fill=approach), stat="identity", position='dodge', width = 0.5) + 
    scale_y_log10() + 
    #geom_errorbar(data=read_only_preprocess, aes(x = as.factor(number_of_operations), ymin = lower_bound, ymax = upper_bound), width=0.2, position=position_dodge(width=0.9)) +
    
    xlab('Scenario') + 
    ylab('Execution Time (log(s))') +
    ggtitle(title) +
    scale_fill_manual('', breaks=c('Rewrite','Preprocess'), 
                      values=c('red','blue')) + 
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.75, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          axis.text.x= element_text(angle=45, hjust=1),
          text=element_text(size=18))
}

results_preprocess_twofields_noindex <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/results 1 - no index/')
results_rewrite_twofields_noindex <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/results rewrite - no index/')
results_preprocess_onefield_noindex <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/results_1 (sem indices)/')

##For detailed table in paper
read_results_summary <- function(folder, heterogeneity_level,number_of_operations) {
  results <- list.files(path=folder, pattern="^result.*\\.txt$", full.names=TRUE) %>%
    lapply(function(file) {
      lines <- readLines(file)
      test_start_line <- lines[grep("^Test Start", lines)]
      last_line <- strsplit(lines[length(lines)], ";")[[1]]
      c(strsplit(test_start_line, ";")[[1]], last_line)
    }) %>%
    do.call(rbind, .) %>%
    as.data.frame(stringsAsFactors = FALSE)
  
  
  colnames(results) <- c("test_start", "method", "insertion_method","number_of_operations", "scenario", "heterogeneity_level", "trial", "t_header", "time_taken")
  results = results[results$heterogeneity_level==heterogeneity_level,]
  results = results[results$number_of_operations==number_of_operations,]
  results$time_taken <- as.numeric(results$time_taken) / as.numeric(results$number_of_operations)
  
  
  results_grouped <- results %>%
    group_by(method, scenario) %>%
    summarise(
      mean_time_taken = mean(as.numeric(time_taken)),
      sd_time_taken = sd(as.numeric(time_taken)),
      count = n()
    ) %>%
    mutate(
      error = qt(0.975, df = count - 1) * sd_time_taken / sqrt(count),
      lower_bound = mean_time_taken - error,
      upper_bound = mean_time_taken + error
    )
  
  return (results_grouped)
}

results_preprocess_twofields_noindex_detailed <- read_results_summary('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - preprocessed noindex/', 0.15, 500)
results_preprocess_twofields_indexed_detailed <- read_results_summary('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - preprocessed index/', 0.15, 500)

results_rewrite_twofields_noindex_detailed <- read_results_summary('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - rewrite noindex/', 0.15, 500)
results_rewrite_twofields_indexed_detailed <- read_results_summary('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - rewrite index/', 0.15, 500)



# Export plots to files
ggsave("plot_execution_time_scenarios_preprocess_0.15.png", plot_execution_time_scenarios(results_preprocess_twofields_noindex, 0.15, ''), width = 10, height = 8)
ggsave("plot_execution_time_scenarios_rewrite_0.15.png", plot_execution_time_scenarios(results_rewrite_twofields_noindex, 0.15, ''), width = 10, height = 8)

ggsave("plot_execution_time_fields_preprocess_write_heavy.png", plot_execution_time_fields(results_preprocess_onefield_noindex, results_preprocess_twofields_noindex, 0.15, 0.05, ''), width = 10, height = 8)
ggsave("plot_execution_time_fields_preprocess_read_heavy.png", plot_execution_time_fields(results_preprocess_onefield_noindex, results_preprocess_twofields_noindex, 0.15, 0.95, ''), width = 10, height = 8)

ggsave("plot_execution_time_heterogeneitylevel_preprocess_write_heavy.png", plot_execution_time_heterogeneitylevel(results_preprocess_twofields_noindex, 0.05, '', 'Write-Heavy'), width = 10, height = 8)
ggsave("plot_execution_time_heterogeneitylevel_preprocess_read_heavy.png", plot_execution_time_heterogeneitylevel(results_preprocess_twofields_noindex, 0.95, '', 'Read-Heavy'), width = 10, height = 8)
ggsave("plot_execution_time_heterogeneitylevel_rewrite_write_heavy.png", plot_execution_time_heterogeneitylevel(results_rewrite_twofields_noindex, 0.05, '', 'Write-Heavy'), width = 10, height = 8)
ggsave("plot_execution_time_heterogeneitylevel_rewrite_read_heavy.png", plot_execution_time_heterogeneitylevel(results_rewrite_twofields_noindex, 0.95, '', 'Read-Heavy'), width = 10, height = 8)

ggsave("plot_execution_time_heterogeneitylevel_bars_preprocess.png", plot_execution_time_heterogeneitylevel_bars(results_preprocess_twofields_noindex, 500, ''), width = 10, height = 8)
ggsave("plot_execution_time_heterogeneitylevel_bars_rewrite.png", plot_execution_time_heterogeneitylevel_bars(results_rewrite_twofields_noindex, 500, ''), width = 10, height = 8)




ggsave("plot_execution_time_methods_500op_30_2F.png", plot_execution_time_methods(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 500, 0.3, '', FALSE), width = 10, height = 8)
ggsave("plot_execution_time_methods_sep_500op_30_2F.png", plot_execution_time_methods(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 500, 0.3, '', TRUE), width = 15, height = 8)

ggsave("plot_execution_time_opsmethod_read_heavy_30_2F.png", plot_execution_time_opsmethod(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.05, 0.3, ''), width = 10, height = 8)
ggsave("plot_execution_time_opsmethod_read_only_30_2F.png", plot_execution_time_opsmethod(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0, 0.3, ''), width = 10, height = 8)
ggsave("plot_execution_time_opsmethod_write_heavy_30_2F.png", plot_execution_time_opsmethod(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.95, 0.3, ''), width = 10, height = 8)
ggsave("plot_execution_time_opsmethod_write_only_30_2F.png", plot_execution_time_opsmethod(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 1, 0.3, ''), width = 10, height = 8)

ggsave("plot_execution_time_hetlevel_read_heavy_500_2F.png", plot_execution_time_hetlevel(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.05, 500, ''), width = 10, height = 8)
ggsave("plot_execution_time_hetlevel_write_heavy_500_2F.png", plot_execution_time_hetlevel(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.95, 500, ''), width = 10, height = 8)
ggsave("plot_execution_time_hetlevel_write_only_500_2F.png", plot_execution_time_hetlevel(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 1, 500, ''), width = 10, height = 8)

ggsave("plot_execution_time_scenarios_approaches_500_2F.png", plot_execution_time_scenarios_bar(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.3, 500, ''), width = 10, height = 8)




plot_execution_indexed <- function(results_noindex, results_indexed, scenario, heterogeneity_level, title) {
  noindexed <- results_noindex[results_noindex$scenario == scenario & results_noindex$heterogeneity_level == heterogeneity_level,]
  indexed <- results_indexed[results_indexed$scenario == scenario & results_indexed$heterogeneity_level == heterogeneity_level,]
  
  
  ggplot() + 
    geom_line(data=noindexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='No-Index')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=noindexed) +
    geom_point(data=noindexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='No-Index'), size=3) + 
    
    geom_line(data=indexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Indexed')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=indexed) +
    geom_point(data=indexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Indexed'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c('No-Index','Indexed'), values=c('red','blue')) + 
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
}


plot_execution_index_methods <- function(results_index_preprocess, results_index_rewrite, scenario, heterogeneity_level, title) {
  preprocess <- results_index_preprocess[results_index_preprocess$scenario == scenario & results_index_preprocess$heterogeneity_level == heterogeneity_level,]
  rewrite <- results_index_rewrite[results_index_rewrite$scenario == scenario & results_index_rewrite$heterogeneity_level == heterogeneity_level,]
  
  
  ggplot() + 
    geom_line(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Preprocess')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=preprocess) +
    geom_point(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Preprocess'), size=3) + 
    
    geom_line(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Rewrite')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound), alpha = 0.2, data=rewrite) +
    geom_point(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, colour='Rewrite'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_colour_manual('', breaks=c('Preprocess','Rewrite'), values=c('red','blue')) + 
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          text=element_text(size=18))
}
plot_execution_time_scenarios_all_bar <- function(results_preprocess_noindex, results_rewrite_noindex, results_preprocess_indexed, results_rewrite_indexed, heterogeneity_level, number_of_operations, title) {
  results_preprocess_noindex$approach = 'Preprocess (No Index)'
  results_rewrite_noindex$approach = 'Rewrite (No Index)'
  results_preprocess_indexed$approach = 'Preprocess (Indexed)'
  results_rewrite_indexed$approach = 'Rewrite (Indexed)'
  
  results = rbind(results_preprocess_noindex,results_rewrite_noindex,results_preprocess_indexed,results_rewrite_indexed)
  results = results[results$number_of_operations==number_of_operations,]
  results$scenario = factor(results$scenario, levels = c(0,0.05,0.5,0.95,1), labels=c("Read-Only","Read-Heavy","50/50","Write-Heavy","Write-Only"))
  
  ggplot() + 
    geom_bar(data=results, aes(x = scenario, y = mean_time_taken, fill=approach), stat="identity", position=position_dodge(width=0.8), width = 0.7) + 
    scale_y_log10() + 
    geom_errorbar(data=results, aes(x = scenario, ymin = lower_bound, ymax = upper_bound, group = approach), 
                  width = 0.2, position=position_dodge(width=0.8), size=1) +
    
    xlab('Scenario') + 
    ylab('Execution Time (log(s))') +
    ggtitle(title) +
    scale_fill_manual('', breaks=c('Preprocess (Indexed)','Preprocess (No Index)','Rewrite (Indexed)','Rewrite (No Index)'), 
                      values=c('lightblue','blue','pink','red')) + 
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = "bottom",
          legend.justification = "center",
          # legend.box.just = "center",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size=25),
          legend.title = element_text(size=20),
          axis.text.x= element_text(angle=45, hjust=1, size=20),
          text=element_text(size=25)) +
    guides(fill=guide_legend(nrow=2, byrow=TRUE))
}


results_preprocess_twofields_noindex <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - preprocessed noindex/')
results_preprocess_twofields_indexed <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - preprocessed index/')

results_rewrite_twofields_noindex <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - rewrite noindex/')
results_rewrite_twofields_indexed <- process_results('/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_indices/results - rewrite index/')

plot_execution_indexed(results_preprocess_twofields_noindex, results_preprocess_twofields_indexed, 0.05, 0.3, '')
plot_execution_indexed(results_rewrite_twofields_noindex, results_rewrite_twofields_indexed, 0.05, 0.3, '')

plot_execution_index_methods(results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.05, 0.3, 'Indexed - Read-Heavy')
plot_execution_index_methods(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.05, 0.3, 'Not-Indexed Read-Heavy')

plot_execution_index_methods(results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.95, 0.3, 'Indexed - Write-Heavy')
plot_execution_index_methods(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0.95, 0.3, 'Not-Indexed Write-Heavy')

plot_execution_time_methods(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 500, 0.3, '', FALSE)
plot_execution_time_methods(results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 500, 0.3, '', FALSE)

plot_execution_time_scenarios_bar(results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.3, 500, '')

plot_execution_time_scenarios_all_bar(results_preprocess_twofields_noindex,results_rewrite_twofields_noindex,results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.3, 500, '')

ggsave("plot_execution_time_scenarios_approaches_indexed_500_2F.png", plot_execution_time_scenarios_bar(results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.3, 500, ''), width = 10, height = 8)
ggsave("plot_execution_time_scenarios_approaches_all_200_2F.png", plot_execution_time_scenarios_all_bar(results_preprocess_twofields_noindex,results_rewrite_twofields_noindex,results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.3, 500, ''), width = 11, height = 8)


### Table

# confidence_interval = function(x) {
#   count = as.numeric(x['count'])
#   dev = as.numeric(x['sd'])
  
#   error = qt(0.95, df=count-1)*dev/sqrt(count)
#   return (error)
# }

# read_only
# baseline = results[results$update_percent == 0,'operations_baseline']
# avg = mean(baseline$operations_baseline)
# dev = sd(baseline$operations_baseline)
# n = length(baseline$operations_baseline)
# error = qt(0.95, df=n-1)*dev/sqrt(n)


# write_only
# summary(results[results$update_percent == 1,'operations_baseline'])

# baseline = results[results$update_percent == 1,'operations_baseline']
# avg = mean(baseline$operations_baseline)
# dev = sd(baseline$operations_baseline)
# n = length(baseline$operations_baseline)
# error = qt(0.95, df=n-1)*dev/sqrt(n)
# error
