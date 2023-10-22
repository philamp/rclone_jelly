
# Fork of "RClone_RD"

## An experimental cache system when using rar2fs over rclone

This is a fork of a fork.
This fork is aimed to be used with jellygrail, an upcoming all-in-one solution to manage your sources of video assets based on a jellyfin package. **This is experimental as it changes the former purpose of rclone's cache.**

Its purpose is to provide a rar2fs index cache and a jellyfin/ffprobe cache to decrease real-debrid ban issues that happens when opening a lot of files and seeking a lot in the same RAR file.

There are 2 modes taking place sequentially when reading a RAR file:

* mode 1: --vfs-cache-mode full normal behavior when the file is being discovered and scanned by jellygrail. **(this is "Read-Write" cache mode)**
  * Jellygrail forces continuous reading of the RAR file to avoid multiple file open requests to the remote (thanks to ``unrar t -sl12582912`` that only reads headers/starting blocks of each file inside the rar + reads files that are less than 10mb, all in the same file open session)
  * So the cache file is filled with useful data for later
* mode 2: When file is finished being scanned, dynamic read-only takes place: it reads from either cache file or remote, depending on slice of data requested. **(this is "Read-Only" cache mode + "Direct source" mode)**. 
Below are few examples:
  * When ffprobe is reading the first 10mb of each file inside a RAR, it reads it directly from rclone cache and does not request it from remote.
  * When rar2fs lists RAR archive contents, it reads it directly from rclone cache and builds its index without requesting the remote for every file. Indeed rar2fs has a file-index cache but its not persistent so this fork also makes up for this. In other words your rar2fs mount can now be killed without loosing the data needed to index what's inside the RAR files...
  * When kodi or jellyfin opens/scans a subtitle file, it reads it completely from cache, avoiding other multiple requests to the remote.

## Other solution planned if possible:

* forking rar2fs to include a persistent cache
* forking rclone with https://github.com/gen2brain/go-unarr to take care of RAR file (+ their cache) directly in rclone

It would avoid the "black magic stuff" such as flaging the file when scanned (to switch from mode 1 to mode 2) and ``unrar -t`` trick

## Notes

this fork can be used with other cloud providers that might do similar throtling/bans

## Data storage

On average, this solution avoids local extraction of RAR files thus takes on your storage less than 1% of the original RAR filesize

# Original README.MD file below:

# NOTICE

### EVENTHOUGH REALDEBRID NOW SUPPORTS WEBDAV, I STILL RECOMMEND USING THIS FORK

Realdebrid has added support for the WebDav protocoll, which makes it mountable through official rclone software.
As of now (19.08) realdebrids webdav implementation does not support torrent file deletion through rclone and limits the amount of torrents displayed to 200. Its also a whole lot slower than my fork. That is because each time a file is accessed through the webdav server, realdebrid only checks the first 1000 direct links from /downloads. If no corresponding direct link is found, which is most likely the case if you have more than 1000 files, the realdebrid webdav will unrestricts the file again. If you refresh your plex library of the mounted drive for example (and you have a library of more than 1000 files), every single file is unrestricted again and again, which takes a very long time. plex will also re-detect intros and do other metadata task every time a file is unrestricted again.

They did mention that torrent file deletion works with other webdav mounting programs, but I recommend using my fork instead of the realdebrid webdav.

# RClone_RD

This RClone Fork contains a Real-Debrid implementation.
Using this version, the entire RealDebrid /torrents directory can be served as a read-only virtual drive. 

A potential use-case for this is serving the /torrent directory over plex, allowing you to build a media library truly unlimted in size. Im working on a project that allows plex to function the same way that Wako,Syncler and other streaming apps do. Check it out on https://github.com/itsToggle/plex_rd

### Capabilities and Limitations:

- Read/Write capabilities are limited to reading files and deleting them. 
- This rclone fork will automatically sort your torrents into 3 subfolder: "shows", "movies" and "default". If a torrent couldnt be classified as a movie or a show, you can find it in the "default" folder.
- There are no server-side traffic or storage limitations.
- This rclone fork will automatically re-activate direct links when they expire after 1 week.
- There is a server-side connection limit, which I believe is 16 parallel connections.

## Installation:

### Docker:
- check out https://hub.docker.com/repository/docker/itstoggle/docker-volume-rclone_rd for more information on the rclone_rd docker version
- check out https://rclone.org/docker/ for more information on general usage

### Windows:

- install winfsp (https://winfsp.dev/)
- download the latest pre-built 'rclone.exe' file from here: https://github.com/itsToggle/rclone_RD/releases

### Mac OSX (community build):

- I can't cross-compile for macOS, so I'm relying on you guys to compile and share the macOS releases :)
- download the latest pre-built 'rclone-darwin' file from here: https://github.com/itsToggle/rclone_RD/releases

### Linux:

- download the latest pre-built 'rclone-linux' file from here: https://github.com/itsToggle/rclone_RD/releases

### Android:
This version is based the latest release from the [rcx github](https://github.com/x0b/rcx). Ive simply replaced the 'librclone.so' file inside the apk with a compiled version of my rclone fork. To be able to install the apk, the app needed to be signed again which i have done with an [apk-signer app](https://play.google.com/store/apps/details?id=com.haibison.apksigner&hl=en&gl=US) from the android playstore, which Im pretty sure isnt malicious. 
To add realdebrid as a remote, simply setup a realdebrid remote on your PC and copy the `rclone.conf` file (`C:\Users\BigSchlong\.config\rclone`) to your android device. Inside the RCX App you can now load your rclone config file. Youre done, you can mount realdebrid on your android device :)

- download the latest pre-built 'apk' file from here: https://github.com/itsToggle/rclone_RD/releases

## Setting up the remote:

0. open a terminal in the download location of your rclone_rd file.
1. configure rclone by running the command 'rclone config' (could be './rclone config' and depending on your os, the filename could be './rclone-linux' or similar. If you get a permission denied error (linux & macos), run 'sudo chmod u+x rclone-linux', adjusted to the filename.)
2. create a new remote by typing 'n'
3. give your remote a name (e.g. 'your-remote')
4. choose '47) realdebrid' as your remote type
5. follow the rest of the prompted instructions, choose 'no advaced configuration'
6. You can mount your newly created remote by running the command 'rclone cmount your-remote: X: --dir-cache-time 10s' (replace 'your-remote' with your remote name, replace 'X' with a drive letter of your choice or replace 'X:' with a destination folder)
7. If you are running my rclone fork on Linux, replace "cmount" with "mount" in the command above.
8. You've successfuly created a virtual drive of your debrid service!

**You can run rclone as a background service by adding the mounting tag '--no-console' (Windows) or '--deamon' (Linux, Mac, etc).**

### Recommended Tags when mounting:

It is recommended to use the tags in this example mounting command: 

'rclone mount torrents: Y: --dir-cache-time 10s'

This will significantly speed up the mounted drive and detect changes faster.

## Building it yourself (Windows)

I really do suggest downloading the pre-built release. But if you want to tinker a bit and built it yourself, here are the steps:
- Download the project files. 
- Install Golang
- To build the project, you need to have MinGW or a different cgo adaptation installed.
- install WinFsp.
- If you dont want to mount the remote as a virtual drive but rather as a dlna server or silimar, use 'go build' to build the project.
- If you do want to mount the remote as a virtual drive, continue:
- Build the project using 'go build -tags cmount'. 
- if that fails on 'fatal error: fuse_common.h missing', you need to do the following steps:
- Locate this folder: C:\Program Files (x86)\WinFsp\inc\fuse - inside you will find the missing files.
- Copy all files to the directory that they are missing from. For me that was: C:\Users\BigSchlong\go\pkg\mod\github.com\winfsp\cgofuse@v1.5.1-0.20220421173602-ce7e5a65cac7\fuse
- Try to build it again

## Building it yourself (Mac/Linux)

- Download the project files
- Install Golang 
- Run a terminal in the root directory of the project files
- use 'go build -tags cmount' to build the project
- If anything fails, Check the official rclone Channels for Help.
- Please feel free to contact me if you have compiled a version, so I can provide it as a comunity build for others :)


