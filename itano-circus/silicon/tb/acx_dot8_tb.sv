`timescale 1ns/1ps

module acx_dot8_tb;
    logic clk;
    logic rst_n;
    logic in_valid;
    logic signed [7:0] a [0:7];
    logic signed [7:0] b [0:7];
    logic [63:0] a_flat;
    logic [63:0] b_flat;
    logic out_valid;
    logic signed [31:0] result;
    integer i;
    integer pack_lane;
    integer failures = 0;

    always #5 clk = ~clk;

    always_comb begin
        for (pack_lane = 0; pack_lane < 8; pack_lane = pack_lane + 1) begin
            a_flat[pack_lane*8 +: 8] = a[pack_lane];
            b_flat[pack_lane*8 +: 8] = b[pack_lane];
        end
    end

    acx_dot8 dut (
        .clk(clk), .rst_n(rst_n), .in_valid(in_valid),
        .a_flat(a_flat), .b_flat(b_flat),
        .out_valid(out_valid), .result(result)
    );

    task automatic apply_and_check(input integer expected);
        begin
            @(negedge clk);
            in_valid = 1;
            @(negedge clk);
            in_valid = 0;
            if (!out_valid || result !== expected) begin
                $display("FAIL expected=%0d actual=%0d valid=%0b", expected, result, out_valid);
                failures = failures + 1;
            end
        end
    endtask

    initial begin
        clk = 0;
        rst_n = 0;
        in_valid = 0;
        for (i = 0; i < 8; i = i + 1) begin a[i] = 0; b[i] = 0; end
        repeat (2) @(negedge clk);
        rst_n = 1;
        apply_and_check(0);

        a[0] = 1; a[1] = 2; a[2] = 3; a[3] = 4;
        a[4] = 5; a[5] = 6; a[6] = 7; a[7] = 8;
        for (i = 0; i < 8; i = i + 1) begin b[i] = 1; end
        apply_and_check(36);

        for (i = 0; i < 8; i = i + 1) begin a[i] = 127; b[i] = 127; end
        apply_and_check(129032);

        for (i = 0; i < 8; i = i + 1) begin a[i] = -128; b[i] = 127; end
        apply_and_check(-130048);

        a[0] = -1; a[1] = -2; a[2] = -3; a[3] = -4;
        a[4] =  5; a[5] =  6; a[6] =  7; a[7] =  8;
        b[0] =  8; b[1] =  7; b[2] =  6; b[3] =  5;
        b[4] =  4; b[5] =  3; b[6] =  2; b[7] =  1;
        apply_and_check(0);

        a[0] = 12; a[1] = -7; a[2] =  3; a[3] = 99;
        a[4] = -128; a[5] = 127; a[6] = 0; a[7] = 4;
        b[0] =  2; b[1] =  5; b[2] = -9; b[3] = -3;
        b[4] =  1; b[5] = -1; b[6] = 44; b[7] = 8;
        apply_and_check(-558);

        if (failures == 0) begin
            $display("PASS ACX-DOT8-R1");
            $finish;
        end else begin
            $fatal(1, "%0d failures", failures);
        end
    end
endmodule
