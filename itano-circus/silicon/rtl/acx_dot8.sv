// ACX-DOT8-R1: one-stage signed INT8 dot-product learning block.
// This module is intentionally small enough to verify before adding scheduling,
// memory, interconnect, or a programming model.
`timescale 1ns/1ps

module acx_dot8 (
    input  logic                    clk,
    input  logic                    rst_n,
    input  logic                    in_valid,
    // Flat little-lane buses keep the boundary portable across the two local
    // RTL tools. Lane N occupies bits N*8 +: 8.
    input  logic [63:0]             a_flat,
    input  logic [63:0]             b_flat,
    output logic                    out_valid,
    output logic signed [31:0]      result
);
    logic signed [31:0] sum_comb;
    integer lane;

    always_comb begin
        sum_comb = 32'sd0;
        for (lane = 0; lane < 8; lane = lane + 1) begin
            sum_comb = sum_comb +
                ($signed(a_flat[lane*8 +: 8]) * $signed(b_flat[lane*8 +: 8]));
        end
    end

    always_ff @(posedge clk or negedge rst_n) begin
        if (!rst_n) begin
            result    <= 32'sd0;
            out_valid <= 1'b0;
        end else begin
            out_valid <= in_valid;
            if (in_valid) begin
                result <= sum_comb;
            end
        end
    end
endmodule
