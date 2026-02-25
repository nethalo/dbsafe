Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 650
Set Theme "Dracula"
Set TypingSpeed 40ms
Set Padding 20

Output assets/dbsafe-json-output.gif

Hide
Type "export DBSAFE_PASSWORD=dbsafe_demo"
Enter
Show

Sleep 500ms

Type `./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo --format json "ALTER TABLE customers DROP COLUMN phone" | jq .`
Sleep 500ms
Enter
Sleep 4s

Screenshot assets/dbsafe-json-output.png
