# Rust convert xlsx.zip to csv.gz
Useful for importing data into Splunk.

## Splunk
- Check timestamp please <= remmember this: timestamp?
- Add settings to source parser: `MAX_DAYS_AGO=99999` , `MAX_TIMESTAMP_LOOKAHEAD=999999`

## Search with `_time`
```
// -7 hour
| eval _time = relative_time(_time, "-7h")

// +7 hour: attributes_event_time is timestamp
| eval _time = strptime(attributes_event_time, "%s%3N") + (7*3600)

// To import result to Excel with VN time
| eval Time_VN = strftime(_time, "%Y-%m-%d %H:%M:%S")

// offhours
| eval hour_str = strftime(_time, "%H:%M")
| eval weekday = strftime(_time, "%A")
| eval Is_OffHours = if(
    (weekday IN ("Saturday", "Sunday")) OR 
    (hour_str < "07:30" OR hour_str >= "20:00") OR 
    (hour_str > "11:30" AND hour_str < "13:30"), 
    1, 0)
| where Is_OffHours = 1
```
