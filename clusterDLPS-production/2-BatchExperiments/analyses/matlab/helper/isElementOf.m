function pos = isElementOf(A, B)
% Find the indeces of elements in A which are elements of B
%   TBD
[ii,jj]=unique(B,'stable');
n=numel(A);
pos = zeros(n,1);
for k = 1:n
    if ~isempty(find(ii == A(k), 1))
        pos(k) = k;
    else
        pos(k) = NaN;
    end
end
pos = rmmissing(pos);
end

