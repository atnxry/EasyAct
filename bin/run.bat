::@setlocal enabledelayedexpansion
@set WorkDir=%~dp0
@cd /d %WorkDir%\..\lib\x64
@set libx64=%cd%
@set libopencv=%libx64%\opencv-3.4.16
@set libqt=%libx64%\Qt-5.12.2
::@set QT_PLUGIN_PATH=%libqt%\plugins\
@set QT_QPA_PLATFORM_PLUGIN_PATH=%libqt%\plugins\platforms
@set libcamera=%libx64%\camera
@cd /d %WorkDir%

@set PATH=%libx64%;%libqt%\bin;%libqt%\lib;%libqt%\plugins;%libopencv%;%libcamera%;%libcamera%\hykon;%libcamera%\pylon;
@TynonViewer_x64.exe 1751215723