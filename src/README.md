## Test script of lab3

### Usage

`./lab3_testing.sh ./Lab3 your_sudo_password`

You need to pass 2 arguments (the lab3 path and your sudo password) to the script, **and that's enough**. 

***Why need sudo privilege:***

For the sake of convenience, we will create several virtual NICs and set some parameters (such as delay, packet loss rate, etc), and these operations require sudo privilege. Ease up, there have no 'dangerous' operations in this script.