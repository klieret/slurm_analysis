#!/bin/bash

check_usage() {
    USER_NAME=$1
    TOTAL_CAP=$2
    PARTITION_NAME=$3
    START_DATE=$4 #$(date +%Y-%m-01)
    END_DATE=$5 #$(date +%Y-%m-%d)

    TOTAL_GPU_HOURS=0.0

    declare -A -g job_gpu_hours
    while IFS='|' read -r jobid alloc_tres elapsed; do

        main_job_id=${jobid%%.*}
        # echo ${job_gpu_hours[@]}
        # echo "Job ID: $jobid - Main Job ID: $main_job_id - AllocTRES: $alloc_tres - Elapsed: $elapsed"  # Debug output

        if [[ -v job_gpu_hours[$main_job_id] ]]; then
            continue
        fi
        GPU_COUNT=$(echo "$alloc_tres" | grep -oP 'gres/gpu=\K[0-9]+' || echo "0")
        if [[ -z "$GPU_COUNT" || "$GPU_COUNT" -eq 0 ]]; then
            continue
        fi

        if [[ "$elapsed" =~ ^([0-9]+)-([0-9]+):([0-9]+):([0-9]+)$ ]]; then
            DAYS=$((10#${BASH_REMATCH[1]}))
            HOURS=$((10#${BASH_REMATCH[2]}))
            MINUTES=$((10#${BASH_REMATCH[3]}))
            SECONDS=$((10#${BASH_REMATCH[4]}))
            TOTAL_HOURS=$(echo "scale=3; ($DAYS * 24 + $HOURS + $MINUTES / 60.0 + $SECONDS / 3600.0)" | bc)
        elif [[ "$elapsed" =~ ^([0-9]+):([0-9]+):([0-9]+)$ ]]; then
            HOURS=$((10#${BASH_REMATCH[1]}))
            MINUTES=$((10#${BASH_REMATCH[2]}))
            SECONDS=$((10#${BASH_REMATCH[3]}))
            TOTAL_HOURS=$(echo "scale=3; ($HOURS + $MINUTES / 60.0 + $SECONDS / 3600.0)" | bc)
        elif [[ "$elapsed" =~ ^([0-9]+):([0-9]+)$ ]]; then
            MINUTES=$((10#${BASH_REMATCH[1]}))
            SECONDS=$((10#${BASH_REMATCH[2]}))
            TOTAL_HOURS=$(echo "scale=3; ($MINUTES / 60.0 + $SECONDS / 3600.0)" | bc)
        else
            echo "Warning: Unrecognized elapsed time format for job $jobid: $elapsed"
            continue
        fi

        GPU_HOURS=$(echo "scale=3; $GPU_COUNT * $TOTAL_HOURS" | bc)

        job_gpu_hours[$main_job_id]=$GPU_HOURS
    done < <(sacct -u "$USER_NAME" -S "$START_DATE" -E "$END_DATE" --partition="$PARTITION_NAME" --format=JobID,AllocTRES,Elapsed --parsable2 --noheader)

    for hours in "${job_gpu_hours[@]}"; do
        TOTAL_GPU_HOURS=$(echo "scale=3; $TOTAL_GPU_HOURS + $hours" | bc)
    done
    TOTAL_GPU_HOURS=$(awk "BEGIN {printf \"%.2f\", $TOTAL_GPU_HOURS}")
    REMAINING_GPU_HOURS=$(awk "BEGIN {printf \"%.3f\", $TOTAL_CAP - $TOTAL_GPU_HOURS}")

    REMAINING_GPU_HOURS=$(awk "BEGIN {printf \"%.2f\", $TOTAL_CAP - $TOTAL_GPU_HOURS}")

    echo -e "User: $USER_NAME\n"
    echo -e "High Priority GPU hrs since $START_DATE:\t $TOTAL_GPU_HOURS hours"
    echo -e "Remaining HP GPU hrs before $(date -d "$START_DATE + 1 month - 1 day" +%Y-%m-%d):\t $REMAINING_GPU_HOURS hours"

    show_progress "$TOTAL_GPU_HOURS" "$TOTAL_CAP" 

    printf "\n"
    if [[ $(echo "$REMAINING_GPU_HOURS < 0" | bc -l) -eq 1 ]]; then
        echo -e "You have exceeded the High priority GPU hour quota!\nPlease stop launching jobs using the $PARTITION_NAME QoS.\n"
    fi
    echo "The quota will be reset to $TOTAL_CAP on $(date -d "$START_DATE + 1 month" +%Y-%m-01)"
}


# Function to display the progress bar
show_progress() {
    if [[ $# -ne 2 ]]; then
        echo "Usage: show_progress CURRENT TOTAL"
        return 1
    fi
    
    if ! local is_valid=$(echo "$1 >= 0 && $2 > 0" | bc -l); then
        echo "Error: CURRENT must be >= 0 and TOTAL must be > 0"
        return 1
    fi

    local current=$1
    local total=$2
    local bar_width=50  # Width of the progress bar in characters
    
    local percentage=$(echo "scale=2; ($current * 100) / $total" | bc)
    percentage=$(echo "if($percentage > 100) 100 else $percentage" | bc)
    
    local percentage_int=$(echo "($percentage + 0.5) / 1" | bc)
    local filled_width=$(echo "($percentage_int * $bar_width) / 100" | bc)
    local empty_width=$((bar_width - filled_width))
    
    local filled_bar=$(printf '█%.0s' $(seq 1 $filled_width))
    if [[ $empty_width -eq 0 ]]; then
        local empty_bar=""
    else
        local empty_bar=$(printf '░%.0s' $(seq 1 $empty_width))
    fi

    # Format numbers with consistent decimal places
    local formatted_current=$(printf "%.1f" $current)
    local formatted_total=$(printf "%.1f" $total)
    
    # Calculate padding for current/total numbers
    local total_len=${#formatted_total}
    local curr_padded=$(printf "%${total_len}s" $formatted_current)
    
    # Clear the line and show the progress bar
    printf "\r\033[K"  # Clear the line
    printf "%s%s %6.2f%% (%s/%s)" \
        "$filled_bar" \
        "$empty_bar" \
        "$percentage" \
        "$curr_padded" \
        "$formatted_total"
    
    printf "\n"
}

# Parse command-line arguments
USER_NAME=$USER
TOTAL_CAP=500.0
PARTITION_NAME="pli-c"
YESTERDAY_TAG=False

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--user)
            USER_NAME="$2"
            shift 2
            ;;
        -c|--cap)
            TOTAL_CAP="$2"
            shift 2
            ;;
        -p|--partition)
            PARTITION_NAME="$2"
            shift 2
            ;;
        -y|--yesterday)
            YESTERDAY_TAG=True
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

TODAY=$(date +%Y-%m-%d-%H:%M:%S)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d-%H:%M:%S)

FIRST_DAY_OF_MONTH=$(date +%Y-%m-01)
if [[ "$YESTERDAY_TAG" == True ]]; then
    START_DATE=$FIRST_DAY_OF_MONTH
    END_DATE=$YESTERDAY
else
    START_DATE=$FIRST_DAY_OF_MONTH
    END_DATE=$TODAY
fi

echo "Start Date: $START_DATE"
echo "End Date: $END_DATE"

check_usage $USER_NAME $TOTAL_CAP $PARTITION_NAME $START_DATE $END_DATE

# 