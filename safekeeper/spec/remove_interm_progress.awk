# Print all lines, but thin out lines starting with Progress:
# leave only first and last 5 ones in the beginning, and only 1 of 1440
# of others (once a day).
# Also remove checkpointing logs.
{
    lines[NR] = $0
}
$0 ~ /^Progress/ {
    ++pcount
}
END {
    progress_idx = 0
    for (i = 1; i <= NR; i++) {
        if (lines[i] ~ /^Progress/) {
            if (progress_idx < 5 || progress_idx >= pcount - 5 || progress_idx % 1440 == 0) {
                print lines[i]
            }
            progress_idx++
        }
        else if (lines[i] ~ /^Checkpointing/) {}
        else {
            print lines[i]
        }
    }
}