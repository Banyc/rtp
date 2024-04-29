local rtp_name = "my_rtp"

local rtp = Proto(rtp_name, rtp_name)

local MAX_NUM_ACK = 128

local ACK_CMD = 0
local DATA_CMD = 1
local KILL_CMD = 2

local pf_ack = {}
for i = 1, MAX_NUM_ACK do
    pf_ack[i] = ProtoField.uint64(rtp_name .. ".ack." .. i, "ack." .. i)
    rtp.fields[#rtp.fields + 1] = pf_ack[i]
end
pf_ack.this = ProtoField.uint64(rtp_name .. ".ack", "ack")
rtp.fields[#rtp.fields + 1] = pf_ack.this

local pf_data = {
    this    = ProtoField.uint64(rtp_name .. ".data", "data"),
    seq     = ProtoField.uint64(rtp_name .. ".data.seq", "seq"),
    payload = ProtoField.bytes(rtp_name .. ".data.payload", "payload"),
}
rtp.fields[#rtp.fields + 1] = pf_data.this
rtp.fields[#rtp.fields + 1] = pf_data.seq
rtp.fields[#rtp.fields + 1] = pf_data.payload

local pf_kill = ProtoField.bool(rtp_name .. ".kill", "kill")
rtp.fields[#rtp.fields + 1] = pf_kill

function rtp.dissector(tvbuf, pktinfo, root)
    pktinfo.cols.protocol:set(rtp_name)

    local pktlen = tvbuf:reported_length_remaining()

    local tree = root:add(rtp, tvbuf:range(0, pktlen))

    local pos = 0
    local ack_index = 1
    local info = ""

    for i = 1, MAX_NUM_ACK do
        if pktlen <= pos then
            break
        end

        local cmd = tvbuf:range(pos, 1)
        pos = pos + 1
        if cmd:uint() == ACK_CMD then
            -- tree:add(pf_ack.this, cmd)
            
            local seq = tvbuf:range(pos, 8)
            pos = pos + 8
            tree:add(pf_ack[ack_index], seq)
            ack_index = ack_index + 1

            info = info .. " Ack=" .. seq:uint64()
        elseif cmd:uint() == DATA_CMD then
            -- tree:add(pf_data.this, cmd)

            local seq = tvbuf:range(pos, 8)
            pos = pos + 8
            tree:add(pf_data.seq, seq)

            local len = tvbuf:range(pos, 2):uint()
            pos = pos + 2

            local payload = tvbuf:range(pos, len)
            pos = pos + len
            tree:add(pf_data.payload, payload)

            info = info .. " Seq=" .. seq:uint64()
        elseif cmd:uint() == KILL_CMD then
            tree:add(pf_kill, cmd)
            
            info = info .. " Kill"
        else
            print(cmd:uint())
            assert(false)
        end
    end

    local orig_info = tostring(pktinfo.cols.info)
    pktinfo.cols.info:set(orig_info .. info)

    return pos
end

for port = 1024, 65535 do
    DissectorTable.get("udp.port"):add(port, rtp)
end
