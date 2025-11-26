Project TODO List:
- [x] Convert to poetry project and remove linux dependency checking from run.py.
- [x] Install pytest using poetry and create test directory.
- [ ] Add more tests.
- [ ] Remove service-mode as an option (and test systemd service file and add systemd timer file.)
- [ ] Rename prune script to prune.py and add unit tests.
- [ ] Module run.py should take arbitrary rclone copy commandline args, e.g., --exclude [stringArray]. See: https://rclone.org/commands/rclone_copy/. And add unit tests.
- [ ] Add unit tests for database support.
- [ ] Write install script and uninstall script.
