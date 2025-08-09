library(dplyr)
library(readr)
library(data.table)
library(ggplot2)
library(scales)
library(ggpattern)

###### FILES PROCESSING #####################################################

process_results <- function(folder) {
  results <- list.files(path = folder, pattern = "^result.*\\.txt$", full.names = TRUE) %>%
    lapply(function(file) {
      lines <- readLines(file)
      test_start_line <- lines[grep("^Test Start", lines)]
      last_line <- strsplit(lines[length(lines)], ";")[[1]]
      c(strsplit(test_start_line, ";")[[1]], last_line)
    }) %>%
    do.call(rbind, .) %>%
    as.data.frame(stringsAsFactors = FALSE)
  
  colnames(results) <- c(
    "test_start", "method", "insertion_method",
    "number_of_operations", "scenario", "heterogeneity_level",
    "trial", "t_header", "time_taken"
  )
  
  results$time_taken <- as.numeric(results$time_taken)  # Ensure it's numeric early
  
  results_grouped <- results %>%
    group_by(method, number_of_operations, scenario, heterogeneity_level) %>%
    group_modify(~ {
      df <- .x
      
      # Remove the two farthest from the mean
      time_vals <- df$time_taken
      mean_time <- mean(time_vals)
      
      distances <- abs(time_vals - mean_time)
      
      if (length(time_vals) > 2) {
        remove_idx <- order(distances, decreasing = TRUE)[1:2]
        time_vals_clean <- time_vals[-remove_idx]
      } else {
        time_vals_clean <- time_vals  # If <= 2 values, keep as is
      }
      
      data.frame(
        mean_time_taken = mean(time_vals_clean),
        sd_time_taken = sd(time_vals_clean),
        count = length(time_vals_clean)
      )
    }) %>%
    mutate(
      error = qt(0.975, df = count - 1) * sd_time_taken / sqrt(count),
      lower_bound = mean_time_taken - error,
      upper_bound = mean_time_taken + error
    )
  
  return(results_grouped)
}


process_results_check_files <- function(folder) {
  files <- list.files(path = folder, pattern = "^result.*\\.txt$", full.names = TRUE)
  
  bad_files <- c()
  good_results <- list()
  
  for (file in files) {
    lines <- readLines(file)
    
    # Check for Test Start line
    test_start_line <- grep("^Test Start", lines, value = TRUE)
    if (length(test_start_line) == 0) {
      bad_files <- c(bad_files, file)
      next
    }
    
    last_line <- lines[length(lines)]
    
    # Split parts
    test_start_parts <- strsplit(test_start_line, ";")[[1]]
    last_line_parts <- strsplit(last_line, ";")[[1]]
    
    combined <- c(test_start_parts, last_line_parts)
    
    if (length(combined) != 9) {
      bad_files <- c(bad_files, file)
      next
    }
    
    good_results[[file]] <- combined
  }
  
  if (length(bad_files) > 0) {
    cat("\n🚨 The following files are problematic:\n")
    print(bad_files)
  } else {
    cat("\n✅ No problematic files found!\n")
  }
}



process_results_operations <- function(folder) {
  results <- list.files(path=folder, pattern="^result.*\\.txt$", full.names=TRUE) %>%
    lapply(function(file) {
      lines <- readLines(file)
      data.frame(
        test_type = sapply(lines, function(line) strsplit(line, ";")[[1]][1]),
        operations_method = sapply(lines, function(line) strsplit(line, ";")[[1]][2]),
        time_taken = as.numeric(sapply(lines, function(line) strsplit(line, ";")[[1]][3])),
        stringsAsFactors = FALSE
      )
    }) %>%
    do.call(rbind, .)
  
  results_grouped <- results %>%
    group_by(operations_method) %>%
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



################################ PLOTS ###############################################################

plot_execution_time_scenarios <- function(results_grouped ,heterogeneity_level, title) {
  read_only <- results_grouped[results_grouped$scenario == 0 & results_grouped$heterogeneity_level == heterogeneity_level,]
  write_only <- results_grouped[results_grouped$scenario == 1 & results_grouped$heterogeneity_level == heterogeneity_level,]
  read_heavy <- results_grouped[results_grouped$scenario == 0.05 & results_grouped$heterogeneity_level == heterogeneity_level,]
  write_heavy <- results_grouped[results_grouped$scenario == 0.95 & results_grouped$heterogeneity_level == heterogeneity_level,]
  mixed <- results_grouped[results_grouped$scenario == 0.5 & results_grouped$heterogeneity_level == heterogeneity_level,]
  
  ggplot() + 
    geom_line(data=read_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Read-Only', linetype='Read-Only', shape='Read-Only')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Read-Only'), alpha = 0.2, data=read_only) +
    geom_point(data=read_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Read-Only', linetype='Read-Only', shape='Read-Only'), size=3) + 
    
    geom_line(data=write_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Write-Only', linetype='Write-Only', shape='Write-Only')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Write-Only'), alpha = 0.2, data=write_only) +
    geom_point(data=write_only, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Write-Only', linetype='Write-Only', shape='Write-Only'), size=3) + 
    
    geom_line(data=read_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Read-Heavy', linetype='Read-Heavy', shape='Read-Heavy')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Read-Heavy'), alpha = 0.2, data=read_heavy) +
    geom_point(data=read_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Read-Heavy', linetype='Read-Heavy', shape='Read-Heavy'), size=3) + 
    
    geom_line(data=write_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Write-Heavy', linetype='Write-Heavy', shape='Write-Heavy')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Write-Heavy'), alpha = 0.2, data=write_heavy) +
    geom_point(data=write_heavy, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Write-Heavy', linetype='Write-Heavy', shape='Write-Heavy'), size=3) + 
    
    geom_line(data=mixed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='50/50', linetype='50/50', shape='50/50')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='50/50'), alpha = 0.2, data=mixed) +
    geom_point(data=mixed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='50/50', linetype='50/50', shape='50/50'), size=3) + 
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_color_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), 
                       values=c('Write-Only'='#E69F00', 'Write-Heavy'='#56B4E9', '50/50'='#009E73', 'Read-Heavy'='#0072B2', 'Read-Only'='#CC79A7')) +
    scale_fill_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), 
                      values=c('Write-Only'='#E69F00', 'Write-Heavy'='#56B4E9', '50/50'='#009E73', 'Read-Heavy'='#0072B2', 'Read-Only'='#CC79A7')) +
    scale_linetype_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), 
                          values=c('solid','dashed','dotted','longdash','twodash')) + 
    scale_shape_manual('', breaks=c('Write-Only','Write-Heavy','50/50','Read-Heavy','Read-Only'), 
                       values=c(16,17,15,18,19)) +
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
    geom_line(data=onefield, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='One Het.Field', linetype='One Het.Field', shape='One Het.Field')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='One Het.Field'), alpha = 0.2, data=onefield) +
    geom_point(data=onefield, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='One Het.Field', linetype='One Het.Field', shape='One Het.Field'), size=3) + 
    
    geom_line(data=twofields, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Two Het. Fields', linetype='Two Het. Fields', shape='Two Het. Fields')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Two Het. Fields'), alpha = 0.2, data=twofields) +
    geom_point(data=twofields, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Two Het. Fields', linetype='Two Het. Fields', shape='Two Het. Fields'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_color_manual('', breaks=c("One Het.Field","Two Het. Fields"), 
                       values=c("One Het.Field"='#E69F00', "Two Het. Fields"='#56B4E9')) +
    scale_fill_manual('', breaks=c("One Het.Field","Two Het. Fields"), 
                      values=c("One Het.Field"='#E69F00', "Two Het. Fields"='#56B4E9')) +
    scale_linetype_manual('', breaks=c("One Het.Field","Two Het. Fields"), 
                          values=c('solid','dashed')) + 
    scale_shape_manual('', breaks=c("One Het.Field","Two Het. Fields"), 
                       values=c(16,17)) +
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
    geom_bar(data=results, aes(x = scenario, y = mean_time_taken, fill=heterogeneity_level), 
             stat="identity", position=position_dodge(width=0.8), width = 0.7) +     
    geom_errorbar(data=results, aes(x = scenario, ymin = lower_bound, ymax = upper_bound, group = heterogeneity_level), 
                  width = 0.2, position=position_dodge(width=0.8), size=1) +
    
    xlab('Scenario') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_fill_manual('', breaks=c('30%','15%'), 
                      values=c('30%' = '#E69F00', '15%' = '#56B4E9')) + 
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = "bottom",
          legend.justification = "center",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size=25),
          legend.title = element_text(size=20),
          axis.text.x= element_text(angle=45, hjust=1, size=20),
          text=element_text(size=25))
}


plot_execution_time_opsmethod <- function(results_grouped_preprocess, results_grouped_rewrite, scenario, heterogeneity_level, title, xlab) {
  preprocess <- results_grouped_preprocess[results_grouped_preprocess$scenario == scenario & results_grouped_preprocess$heterogeneity_level == heterogeneity_level,]
  rewrite <- results_grouped_rewrite[results_grouped_rewrite$scenario == scenario & results_grouped_rewrite$heterogeneity_level == heterogeneity_level,]
  
  
  ggplot() + 
    geom_line(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Preprocess', linetype='Preprocess', shape='Preprocess')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Preprocess'), alpha = 0.2, data=preprocess) +
    geom_point(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Preprocess', linetype='Preprocess', shape='Preprocess'), size=3) + 
    
    geom_line(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Rewrite', linetype='Rewrite', shape='Rewrite')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Rewrite'), alpha = 0.2, data=rewrite) +
    geom_point(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Rewrite', linetype='Rewrite', shape='Rewrite'), size=3) +    
    
    
    xlab(xlab) + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_color_manual('', breaks=c('Preprocess','Rewrite'), 
                       values=c('Preprocess'='#E69F00', 'Rewrite'='#56B4E9')) +
    scale_fill_manual('', breaks=c('Preprocess','Rewrite'), 
                      values=c('Preprocess'='#E69F00', 'Rewrite'='#56B4E9')) +
    scale_linetype_manual('', breaks=c('Preprocess','Rewrite'), 
                          values=c('solid','dashed')) + 
    scale_shape_manual('', breaks=c('Preprocess','Rewrite'), 
                       values=c(16,17)) +
    scale_x_continuous(breaks=c(100,200,300,400,500,600,700,800,900)) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = c(.05, .95),
          legend.justification = c("left", "top"),
          legend.box.just = "left",
          legend.margin = margin(6, 6, 6, 6),
          legend.text= element_text(size=23),
          text=element_text(size=25))
}



plot_execution_indexed <- function(results_noindex, results_indexed, scenario, heterogeneity_level, title) {
  noindexed <- results_noindex[results_noindex$scenario == scenario & results_noindex$heterogeneity_level == heterogeneity_level,]
  indexed <- results_indexed[results_indexed$scenario == scenario & results_indexed$heterogeneity_level == heterogeneity_level,]
  
  
  ggplot() + 
    geom_line(data=noindexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='No-Index', linetype='No-Index', shape='No-Index')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='No-Index'), alpha = 0.2, data=noindexed) +
    geom_point(data=noindexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='No-Index', linetype='No-Index', shape='No-Index'), size=3) + 
    
    geom_line(data=indexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Indexed', linetype='Indexed', shape='Indexed')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Indexed'), alpha = 0.2, data=indexed) +
    geom_point(data=indexed, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Indexed', linetype='Indexed', shape='Indexed'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_color_manual('', breaks=c('No-Index','Indexed'), 
                       values=c('No-Index'='#E69F00', 'Indexed'='#56B4E9')) +
    scale_fill_manual('', breaks=c('No-Index','Indexed'), 
                      values=c('No-Index'='#E69F00', 'Indexed'='#56B4E9')) +
    scale_linetype_manual('', breaks=c('No-Index','Indexed'), 
                          values=c('solid','dashed')) + 
    scale_shape_manual('', breaks=c('No-Index','Indexed'), 
                       values=c(16,17)) +
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
    geom_line(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Preprocess', linetype='Preprocess', shape='Preprocess')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Preprocess'), alpha = 0.2, data=preprocess) +
    geom_point(data=preprocess, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Preprocess', linetype='Preprocess', shape='Preprocess'), size=3) + 
    
    geom_line(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Rewrite', linetype='Rewrite', shape='Rewrite')) + 
    geom_ribbon(aes(x=as.numeric(number_of_operations), y=mean_time_taken, ymin = lower_bound, ymax = upper_bound, fill='Rewrite'), alpha = 0.2, data=rewrite) +
    geom_point(data=rewrite, aes(x = as.numeric(number_of_operations), y = mean_time_taken, color='Rewrite', linetype='Rewrite', shape='Rewrite'), size=3) +    
    
    
    xlab('Number of insert/select Operations') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_color_manual('', breaks=c('Preprocess','Rewrite'), 
                       values=c('Preprocess'='#E69F00', 'Rewrite'='#56B4E9')) +
    scale_fill_manual('', breaks=c('Preprocess','Rewrite'), 
                      values=c('Preprocess'='#E69F00', 'Rewrite'='#56B4E9')) +
    scale_linetype_manual('', breaks=c('Preprocess','Rewrite'), 
                          values=c('solid','dashed')) + 
    scale_shape_manual('', breaks=c('Preprocess','Rewrite'), 
                       values=c(16,17)) +
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
    geom_bar(data=results, aes(x = scenario, y = mean_time_taken, fill=approach), 
             stat="identity", position=position_dodge(width=0.8), width = 0.7) + 
    scale_y_log10() + 
    geom_errorbar(data=results, aes(x = scenario, ymin = lower_bound, ymax = upper_bound, group = approach), 
                  width = 0.2, position=position_dodge(width=0.8), size=1) +
    
    xlab('Scenario') + 
    ylab('Execution Time (log(s))') +
    ggtitle(title) +
    scale_fill_manual('', breaks=c('Preprocess (Indexed)','Preprocess (No Index)','Rewrite (Indexed)','Rewrite (No Index)'), 
                      values=c('Preprocess (Indexed)' = '#E69F00', 'Preprocess (No Index)' = '#56B4E9', 'Rewrite (Indexed)' = '#009E73', 'Rewrite (No Index)' = '#F0E442')) + 
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = "bottom",
          legend.justification = "center",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size=25),
          legend.title = element_text(size=20),
          axis.text.x= element_text(angle=45, hjust=1, size=20),
          text=element_text(size=25)) +
    guides(fill=guide_legend(nrow=2, byrow=TRUE))
}


plot_operations_method_performance <- function(results_operations) {
  ggplot(results_operations, aes(x = operations_method, y = mean_time_taken, fill = operations_method)) +
    geom_bar(stat = "identity", position = position_dodge(width = 0.8), width = 0.7) +
    geom_errorbar(aes(ymin = lower_bound, ymax = upper_bound), width = 0.2, position = position_dodge(width = 0.8), size = 1) +
    xlab('') +
    ylab('Mean Execution Time (s)') +
    ggtitle('') +
    scale_fill_manual('', breaks = c('insertion_first', 'operations_first'), 
                      labels = c('Insertion First', 'Operations First'), 
                      values = c('insertion_first' = '#E69F00', 'operations_first' = '#56B4E9')) +
    theme(panel.background = element_rect(fill = 'white', colour = 'black'),
          legend.position = "bottom",
          legend.justification = "center",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size = 12),
          axis.text.x = element_blank(), # Remove category ticks
          axis.ticks.x = element_blank(), # Remove tick marks
          text = element_text(size = 14)) +
    guides(fill = guide_legend(nrow = 1, byrow = TRUE)) 
}

plot_operations_method_performance_all_bar <- function(results_noindex, results_indexed, title) {
  results_indexed[results_indexed$operations_method=='operations_first',]$operations_method = 'Operations First (Indexed)'
  results_indexed[results_indexed$operations_method=='insertion_first',]$operations_method = 'Insertion First (Indexed)'
  results_noindex[results_noindex$operations_method=='operations_first',]$operations_method = 'Operations First (No Index)'
  results_noindex[results_noindex$operations_method=='insertion_first',]$operations_method = 'Insertion First (No Index)'
  
  results = rbind(results_noindex,results_indexed)
  
  ggplot() + 
    geom_bar(data=results, aes(x = operations_method, y = mean_time_taken, fill=operations_method), 
             stat="identity", position=position_dodge(width=0.8), width = 0.7) + 
    geom_errorbar(data=results, aes(x = operations_method, ymin = lower_bound, ymax = upper_bound, group = operations_method), 
                  width = 0.2, position=position_dodge(width=0.8), size=1) +
    
    xlab('') + 
    ylab('Execution Time (s)') +
    ggtitle(title) +
    scale_fill_manual('', breaks=c('Operations First (Indexed)','Operations First (No Index)','Insertion First (Indexed)','Insertion First (No Index)'), 
                      values=c('Operations First (Indexed)' = '#E69F00', 'Operations First (No Index)' = '#56B4E9', 'Insertion First (Indexed)' = '#009E73', 'Insertion First (No Index)' = '#F0E442')) + 
    theme(panel.background = element_rect(fill = 'white', colour = 'black'), 
          legend.position = "bottom",
          legend.justification = "center",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size=20),
          legend.title = element_text(size=20),
          axis.text.x = element_blank(), # Remove category ticks
          axis.ticks.x = element_blank(), # Remove tick marks
          text=element_text(size=20)) +
    guides(fill=guide_legend(nrow=2, byrow=TRUE))
}

###############################################################


################################ TABLE GENERATOR ##################################################

## For detailed table in paper
read_results_summary <- function(folder, heterogeneity_level, number_of_operations) {
  results <- list.files(path = folder, pattern = "^result.*\\.txt$", full.names = TRUE) %>%
    lapply(function(file) {
      lines <- readLines(file)
      test_start_line <- lines[grep("^Test Start", lines)]
      last_line <- strsplit(lines[length(lines)], ";")[[1]]
      c(strsplit(test_start_line, ";")[[1]], last_line)
    }) %>%
    do.call(rbind, .) %>%
    as.data.frame(stringsAsFactors = FALSE)
  
  colnames(results) <- c(
    "test_start", "method", "insertion_method",
    "number_of_operations", "scenario", "heterogeneity_level",
    "trial", "t_header", "time_taken"
  )
  
  # Filter based on parameters
  results <- results[
    results$heterogeneity_level == heterogeneity_level &
      results$number_of_operations == number_of_operations, 
  ]
  
  results$time_taken <- as.numeric(results$time_taken) / as.numeric(results$number_of_operations)
  
  results_grouped <- results %>%
    group_by(method, scenario) %>%
    group_modify(~ {
      df <- .x
      
      time_vals <- df$time_taken
      mean_time <- mean(time_vals)
      
      distances <- abs(time_vals - mean_time)
      
      if (length(time_vals) > 2) {
        remove_idx <- order(distances, decreasing = TRUE)[1:2]
        time_vals_clean <- time_vals[-remove_idx]
      } else {
        time_vals_clean <- time_vals  # If <= 2 points, don't remove anything
      }
      
      data.frame(
        mean_time_taken = mean(time_vals_clean),
        sd_time_taken = sd(time_vals_clean),
        count = length(time_vals_clean)
      )
    }) %>%
    mutate(
      error = qt(0.975, df = count - 1) * sd_time_taken / sqrt(count),
      lower_bound = mean_time_taken - error,
      upper_bound = mean_time_taken + error
    )
  
  return(results_grouped)
}


############################################################################3

results_preprocess_twofields_noindex <- process_results('first experiment/preprocess/')
results_rewrite_twofields_noindex <- process_results('first experiment/rewrite/')

results_preprocess_twofields_noindex_detailed <- read_results_summary('indexes experiment/preprocess/no index/', 0.15, 500)
results_preprocess_twofields_indexed_detailed <- read_results_summary('indexes experiment/preprocess/indexed/', 0.15, 500)

results_rewrite_twofields_noindex_detailed <- read_results_summary('indexes experiment/rewrite/no index/', 0.15, 500)
results_rewrite_twofields_indexed_detailed <- read_results_summary('indexes experiment/rewrite/indexed/', 0.15, 500)


# Export plots to files
ggsave("plot_execution_time_scenarios_preprocess_0.15.png", plot_execution_time_scenarios(results_preprocess_twofields_noindex, 0.15, ''), width = 10, height = 8)
ggsave("plot_execution_time_scenarios_rewrite_0.15.png", plot_execution_time_scenarios(results_rewrite_twofields_noindex, 0.15, ''), width = 10, height = 8)
ggsave("plot_execution_time_heterogeneitylevel_bars_preprocess.png", plot_execution_time_heterogeneitylevel_bars(results_preprocess_twofields_noindex, 500, ''), width = 10, height = 8)
ggsave("plot_execution_time_heterogeneitylevel_bars_rewrite.png", plot_execution_time_heterogeneitylevel_bars(results_rewrite_twofields_noindex, 500, ''), width = 10, height = 8)
ggsave("plot_execution_time_opsmethod_read_only_30_2F.png", plot_execution_time_opsmethod(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 0, 0.3, '', 'Number of select operations'), width = 10, height = 8)
ggsave("plot_execution_time_opsmethod_write_only_30_2F.png", plot_execution_time_opsmethod(results_preprocess_twofields_noindex, results_rewrite_twofields_noindex, 1, 0.3, '', 'Number of insert operations'), width = 10, height = 8)


results_preprocess_twofields_noindex <- process_results('indexes experiment/preprocess/no index/')
results_preprocess_twofields_indexed <- process_results('indexes experiment/preprocess/indexed/')

results_rewrite_twofields_noindex <- process_results('indexes experiment/rewrite/no index/')
results_rewrite_twofields_indexed <- process_results('indexes experiment/rewrite/indexed/')


ggsave("plot_execution_time_scenarios_approaches_all_200_2F.png", plot_execution_time_scenarios_all_bar(results_preprocess_twofields_noindex,results_rewrite_twofields_noindex,results_preprocess_twofields_indexed, results_rewrite_twofields_indexed, 0.3, 500, ''), width = 11, height = 8)


results_operations_indexed <- process_results_operations('initialization experiment/indexed/')
results_operations_not_indexed <- process_results_operations('initialization experiment/no index/')

# Call the function and save the plot
ggsave("operations_method_performance.png", plot_operations_method_performance_all_bar(results_operations_not_indexed,results_operations_indexed,''), width = 10, height = 8)



