from typing import List


class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:
        insertIndex = 1
        for i in range(1, len(nums)):

            if nums[i] != nums[i - 1]:
                nums[insertIndex] = nums[i]
                insertIndex += 1
                print(nums)
        return insertIndex

sol=Solution()
print(sol.removeDuplicates([1,2,2,3]))
