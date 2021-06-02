#!/bin/bash
file=CHANGELOG.md
ok=0
while IFS= read -r line
do
  if [[ "$line" = "## v"* ]] && [ $ok -eq 0 ]; then
    ok=1
    continue
  fi
  if [[ "$line" = "## v"* ]] && [ $ok -eq 1 ]; then
    break
  fi
  if [ "$line" != "" ] && [[ "$line" != "<"* ]]; then
    echo "$line"
  fi
done < "$file"
