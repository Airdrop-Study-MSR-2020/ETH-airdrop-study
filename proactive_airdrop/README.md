#Algorithm 1
The programs in this folder is related to the Proactive airdrop in the paper.

First, you should get the related possible airdrop periods by using the burst detection method. The input file is the file that contains all the token address. Since you have collected all the token transfer event, you could get the token address easily.

```
python burst_detection_airdrop_by_maximum.py token_address.csv
```

Then, remove ico account. We collect the ICO account before, but the data is related to our next topic, so we omit it here. But you could still filter the ICO by using the method we declare in the paper. $potential_airdroppers.csv$ is the result from the previous program

```
python remove_ico_account_from_airdropper.py ico_accounts.csv potential_airdroppers.csv 
```

Then, remove the account that trade with more than one token. $step1_airdropper.csv$ is the result of the previous program
```
python remove_trade_more_than_one_token.py step1_airdropper.csv
```

Remove outliers. $step2_airdropper.csv$ is the result of the previous program
```
python remove_outliers.py step2_airdropper.csv
```

Remove failed periods $final_version_airdrop2.csv$ is the result of the previous program
```
python remove_failed_period.py final_version_airdrop2.csv
```
