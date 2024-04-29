let config_dir = $"($env.HOME)/.config/wireshark/plugins"
let plugin = "rtp.dissector.lua"

mkdir -v $config_dir
cp -v $plugin $config_dir
