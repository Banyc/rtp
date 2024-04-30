local NAME = "my_rtp"

local rtp = Proto(NAME, NAME)

local MAX_NUM_ACK = 128

local ACK_CMD = 0
local DATA_CMD = 1
local KILL_CMD = 2

local pf_ack = {}
for i = 1, MAX_NUM_ACK do
    pf_ack[i] = {
        start = ProtoField.uint64(NAME .. ".ack." .. i .. ".start", "ack." .. i .. ".start"),
        size = ProtoField.uint64(NAME .. ".ack." .. i .. ".size", "ack." .. i .. ".size")
    }
    rtp.fields[#rtp.fields + 1] = pf_ack[i].start
    rtp.fields[#rtp.fields + 1] = pf_ack[i].size
end
pf_ack.this = ProtoField.uint64(NAME .. ".ack", "ack")
rtp.fields[#rtp.fields + 1] = pf_ack.this

local pf_data = {
    this = ProtoField.uint64(NAME .. ".data", "data"),
    seq = ProtoField.uint64(NAME .. ".data.seq", "seq"),
    fin = ProtoField.bool(NAME .. ".data.fin", "fin"),
    payload = ProtoField.bytes(NAME .. ".data.payload", "payload")
}
rtp.fields[#rtp.fields + 1] = pf_data.this
rtp.fields[#rtp.fields + 1] = pf_data.seq
rtp.fields[#rtp.fields + 1] = pf_data.fin
rtp.fields[#rtp.fields + 1] = pf_data.payload

local pf_kill = ProtoField.bool(NAME .. ".kill", "kill")
rtp.fields[#rtp.fields + 1] = pf_kill

local ef_out_of_order = ProtoExpert.new(NAME .. ".data.seq.out_of_order.expert", "Seq out of order",
    expert.group.COMMENTS_GROUP, expert.severity.NOTE)
out_of_order = {
    last_seq = {},
    packets = {}
}

rtp.experts = {ef_out_of_order}

function five_tuple_string(pktinfo)
    local key = tostring(pktinfo.net_src) .. ":" .. tostring(pktinfo.src_port) .. ":" .. tostring(pktinfo.net_dst) ..
                    ":" .. tostring(pktinfo.dst_port)
    return key
end

function rtp.dissector(tvbuf, pktinfo, root)
    pktinfo.cols.protocol:set(NAME)

    local pktlen = tvbuf:reported_length_remaining()

    local tree = root:add(rtp, tvbuf:range(0, pktlen))

    local pos = 0
    local ack_index = 1
    local info = ""
    local key = five_tuple_string(pktinfo)

    for i = 1, MAX_NUM_ACK do
        if pktlen <= pos then
            break
        end

        local cmd = tvbuf:range(pos, 1)
        pos = pos + 1
        if cmd:uint() == ACK_CMD then
            tree:add(pf_ack.this, cmd)

            local start = tvbuf:range(pos, 8)
            pos = pos + 8
            tree:add(pf_ack[ack_index].start, start)
            local size = tvbuf:range(pos, 8)
            pos = pos + 8
            tree:add(pf_ack[ack_index].size, size)
            ack_index = ack_index + 1

            info = info .. " Ack=" .. start:uint64() .. "," .. size:uint64()
        elseif cmd:uint() == DATA_CMD then
            tree:add(pf_data.this, cmd)

            local seq = tvbuf:range(pos, 8)
            pos = pos + 8
            tree:add(pf_data.seq, seq)

            local len = tvbuf:range(pos, 2)
            pos = pos + 2

            if len:uint() == 0 then
                tree:add(pf_data.fin, len)

                info = info .. " Fin=" .. seq:uint64()
            else
                local payload = tvbuf:range(pos, len:uint())
                pos = pos + len:uint()
                tree:add(pf_data.payload, payload)

                info = info .. " Seq=" .. seq:uint64()
            end

            -- out of order
            if not pktinfo.visited then
                if not (out_of_order.last_seq[key] == nil) and seq:uint64() <= out_of_order.last_seq[key] then
                    out_of_order.packets[pktinfo.number] = true
                else
                    out_of_order.last_seq[key] = seq:uint64()
                end
            end
            if out_of_order.packets[pktinfo.number] then
                -- print("out of order", key, seq:uint64(), out_of_order.last_seq[key])
                tree:add_tvb_expert_info(ef_out_of_order, seq)
                info = info .. " [out of order]"
            end
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
