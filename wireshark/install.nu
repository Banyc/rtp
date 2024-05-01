let config_dir = $"($env.HOME)/.config/wireshark/plugins"
let plugin = "rtp.dissector.lua"

mkdir --verbose $config_dir
cp --verbose $plugin $config_dir
