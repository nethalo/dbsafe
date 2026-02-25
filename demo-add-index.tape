Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 800
Set Theme "Dracula"
Set TypingSpeed 40ms
Set Padding 20

Output assets/dbsafe-add-index.gif

Hide
Type "export DBSAFE_PASSWORD=dbsafe_demo"
Enter
Show

Sleep 500ms

Type `./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo "ALTER TABLE orders ADD INDEX idx_created (created_at)"`
Sleep 500ms
Enter
Sleep 4s

Screenshot assets/dbsafe-add-index.png
