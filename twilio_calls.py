from twilio.rest import Client
import csv

client = Client(account_sid, auth_token)

listofdata= client.calls.list(start_time='2019-06-26 17:00:00', end_time='2019-06-26 18:00:00')
newdataset =[]
for i in listofdata:
    newdataset.append((i.account_sid, i.annotation,i.answered_by,i.api_version,i.caller_name,i.date_created,i.date_updated, i.direction, i.duration,i.end_time,i.forwarded_from,i.from_,i.from_formatted,i.group_sid,i.parent_call_sid,i.phone_number_sid,i.price,i.price_unit,i.sid,i.start_time,i.status,i.subresource_uris,i.to,i.to_formatted,i.uri))
with open('twiliodataingestiontestfile.csv','w') as f:
        csv_writer = csv.writer(f)
        for rows in newdataset:
            csv_writer.writerow(rows)

