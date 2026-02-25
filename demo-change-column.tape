Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 1300
Set Theme "Dracula"
Set TypingSpeed 40ms
Set Padding 20

Output assets/dbsafe-change-column.gif

Hide
Type "export DBSAFE_PASSWORD=dbsafe_demo"
Enter
Show

Sleep 500ms

Type `./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo "ALTER TABLE orders CHANGE COLUMN total_amount amount DECIMAL(14,4)"`
Sleep 500ms
Enter
Sleep 4s

Screenshot assets/dbsafe-change-column.png
