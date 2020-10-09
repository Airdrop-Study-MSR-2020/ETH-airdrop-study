#The programs here is to get the automatic controller

#Usage
```
python get_transfer_time.py controlled_accounts.csv transfer_time.csv
```
To get the transfer time of the controlled accounts


```
python get_automatic_controller.py controller_list.csv T N
```
controller_list.csv: this is the file that contains the controller, controlled_account and so on. We get the file from the "get_controller.py", which is in the upper folder
T: this is the Time gap
N: Automatic controller should transfer at least N accounts to it within T seconds.

```
python get_the_self_destruct.py automatic_controllers.csv
```
This is to get different kinds of automatic controller. We modify the "geth" client to collect the self_destruct accounts and put it into the neo4j. We omit the program here.
